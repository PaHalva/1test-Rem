# ============================================================
# Remanga AutoBattle Telegram Bot — single file
# ============================================================
# Локально:
#   pip install -r requirements.txt
#   python bot.py
#
# На PaaS (relaxdev и т.п.):
#   ENV: BOT_TOKEN, FERNET_KEY, DB_PATH, PORT
# ============================================================

import asyncio
import logging
import os
import socket
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import TCPConnector, web
from cryptography.fernet import Fernet
from dotenv import load_dotenv


# ============================================================
# 1. ЛОГИРОВАНИЕ (ставим ДО всего остального)
# ============================================================

# Приглушаем httpx/aiohttp — не хотим видеть каждый запрос на INFO
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("remanga-bot")


# ============================================================
# 2. КОНФИГ
# ============================================================

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

BOT_TOKEN  = os.environ.get("BOT_TOKEN", "").strip()
FERNET_KEY = os.environ.get("FERNET_KEY", "").strip()
DB_PATH    = os.environ.get("DB_PATH", "remanga_bot.db").strip()
PORT       = int(os.environ.get("PORT", "8080"))

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не задан.\n"
        "• Локально: создайте .env с BOT_TOKEN=... рядом с bot.py\n"
        "• На PaaS: добавьте переменную окружения BOT_TOKEN"
    )
if not FERNET_KEY:
    raise RuntimeError(
        "FERNET_KEY не задан. Сгенерируйте:\n"
        "  python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    )

try:
    fernet = Fernet(FERNET_KEY.encode())
except Exception as e:
    raise RuntimeError(f"FERNET_KEY некорректен: {e}")

Path(DB_PATH).resolve().parent.mkdir(parents=True, exist_ok=True)

API_DOMAIN = "https://api.remanga.org"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)

PVP_INTERVAL      = 32
RAID_INTERVAL     = 300
CATACOMB_INTERVAL = 65
LOOP_SLEEP        = 5

RAID_ENERGY_COST = {1: 4, 2: 5, 3: 6, 4: 7, 5: 8, 6: 9, 7: 10, 8: 11, 9: 12, 10: 13}

# ------------------------------------------------------------
# МАГАЗИН
# ------------------------------------------------------------
SHOP_AWAKENING_ENERGY_ID = 6333    # 35 points = 1★
SHOP_COST_EVENT_POINTS   = 35
FORBIDDEN_SHOP_IDS = {6332}        # restore-energy — НИКОГДА

# Автопокупка звёзд выключена. Только фарм рейдами, потолок — 6.
MAX_STARS_FOR_RAID = 6

# ------------------------------------------------------------
# КАТАКОМБЫ: ОКНО И ЛИМИТ (МСК)
# ------------------------------------------------------------
MSK = timezone(timedelta(hours=3))
CATA_WINDOW_START_HOUR = 3     # 03:00 МСК
CATA_WINDOW_END_HOUR   = 23    # 23:00 МСК
CATA_RESURRECT_LIMIT   = 3     # по умолчанию лимит "resurrect" в день


def msk_now() -> datetime:
    return datetime.now(MSK)


def msk_today_str() -> str:
    return msk_now().strftime("%Y-%m-%d")


def msk_in_cata_window() -> bool:
    h = msk_now().hour
    return CATA_WINDOW_START_HOUR <= h < CATA_WINDOW_END_HOUR


def msk_seconds_to_window_start() -> int:
    now = msk_now()
    target = now.replace(hour=CATA_WINDOW_START_HOUR, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return int((target - now).total_seconds())


# ============================================================
# 3. БАЗА ДАННЫХ
# ============================================================

INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id          INTEGER PRIMARY KEY,
    login_enc        BLOB NOT NULL,
    password_enc     BLOB NOT NULL,
    token_enc        BLOB,
    raid_enabled     INTEGER DEFAULT 1,
    pvp_enabled      INTEGER DEFAULT 1,
    cata_enabled     INTEGER DEFAULT 1,
    raid_loc         INTEGER DEFAULT 10,
    cata_fixed       INTEGER DEFAULT 1,
    cata_level       INTEGER DEFAULT 17,
    cata_stars       INTEGER DEFAULT 1,
    created_at       INTEGER DEFAULT (strftime('%s','now'))
);
"""

MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN cata_fixed INTEGER DEFAULT 1",
    "ALTER TABLE users ADD COLUMN cata_level INTEGER DEFAULT 17",
    "ALTER TABLE users ADD COLUMN cata_stars INTEGER DEFAULT 1",
]


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
        for sql in MIGRATIONS:
            try:
                await db.execute(sql)
            except Exception:
                pass
        await db.commit()
    log.info("DB ready at %s", DB_PATH)


async def save_user(chat_id: int, login: str, password: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO users (chat_id, login_enc, password_enc)
               VALUES (?, ?, ?)
               ON CONFLICT(chat_id) DO UPDATE SET
                 login_enc=excluded.login_enc,
                 password_enc=excluded.password_enc""",
            (chat_id, fernet.encrypt(login.encode()), fernet.encrypt(password.encode())),
        )
        await db.commit()


async def save_token(chat_id: int, token: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET token_enc=? WHERE chat_id=?",
            (fernet.encrypt(token.encode()), chat_id),
        )
        await db.commit()


async def get_user(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            d = dict(row)
            return {
                "chat_id":      d["chat_id"],
                "login":        fernet.decrypt(d["login_enc"]).decode(),
                "password":     fernet.decrypt(d["password_enc"]).decode(),
                "token":        fernet.decrypt(d["token_enc"]).decode() if d.get("token_enc") else None,
                "raid_enabled": bool(d.get("raid_enabled", 1)),
                "pvp_enabled":  bool(d.get("pvp_enabled", 1)),
                "cata_enabled": bool(d.get("cata_enabled", 1)),
                "raid_loc":     d.get("raid_loc", 10),
                "cata_fixed":   bool(d.get("cata_fixed", 1)),
                "cata_level":   d.get("cata_level", 17),
                "cata_stars":   d.get("cata_stars", 1),
            }


ALLOWED_FLAG_FIELDS = {
    "raid_enabled", "pvp_enabled", "cata_enabled", "raid_loc",
    "cata_fixed", "cata_level", "cata_stars",
}


async def update_flag(chat_id: int, field: str, value):
    assert field in ALLOWED_FLAG_FIELDS, f"bad field {field}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE users SET {field}=? WHERE chat_id=?", (value, chat_id))
        await db.commit()


async def delete_user(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE chat_id=?", (chat_id,))
        await db.commit()


async def all_chat_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT chat_id FROM users") as cur:
            return [r[0] for r in await cur.fetchall()]


# ============================================================
# 4. КЛИЕНТ REMANGA
# ============================================================

class RemangaAuthError(Exception):
    pass


def _extract_token(data, depth: int = 0):
    if depth > 4 or not isinstance(data, (dict, list)):
        return None
    if isinstance(data, list):
        for item in data:
            t = _extract_token(item, depth + 1)
            if t:
                return t
        return None
    for key in (
        "token", "access_token", "accessToken", "access",
        "jwt", "auth_token", "authToken", "key", "id_token",
        "bearer", "bearer_token", "bearerToken",
    ):
        v = data.get(key)
        if isinstance(v, str) and len(v) > 20:
            return v
    for wrapper in ("content", "data", "result", "user", "profile", "auth"):
        w = data.get(wrapper)
        if isinstance(w, (dict, list)):
            t = _extract_token(w, depth + 1)
            if t:
                return t
    for v in data.values():
        if isinstance(v, (dict, list)):
            t = _extract_token(v, depth + 1)
            if t:
                return t
    return None


class RemangaClient:
    def __init__(self, token: str | None = None):
        self.token = self._norm(token) if token else None
        self._c = httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )

    @staticmethod
    def _norm(t: str) -> str:
        t = t.strip()
        return t if t.startswith("Bearer ") else f"Bearer {t}"

    async def close(self):
        await self._c.aclose()

    def _h(self):
        return {"Authorization": self.token} if self.token else {}

    # ---------- AUTH ----------

    @staticmethod
    async def login(login: str, password: str) -> str:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                f"{API_DOMAIN}/api/users/login/",
                json={"user": login, "password": password},
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            # 🔒 Никаких сырых данных в логах — только статус и длина
            log.info("LOGIN status=%s body_len=%d", r.status_code, len(r.text or ""))

            if r.status_code != 200:
                if r.status_code in (400, 401, 403):
                    raise RemangaAuthError(f"Неверный логин/пароль (HTTP {r.status_code})")
                raise RemangaAuthError(f"Ошибка входа: HTTP {r.status_code}")

            try:
                data = r.json()
            except Exception:
                data = {}

            token = _extract_token(data)
            if not token:
                for cname in ("token", "access_token", "accessToken",
                              "authorization", "jwt", "auth"):
                    try:
                        val = r.cookies.get(cname)
                    except Exception:
                        val = None
                    if val and len(val) > 20:
                        token = val
                        break
            if not token:
                auth_hdr = r.headers.get("authorization") or r.headers.get("Authorization")
                if auth_hdr and auth_hdr.lower().startswith("bearer "):
                    token = auth_hdr[7:]
            if not token:
                raise RemangaAuthError("Сервер вернул 200, но токен не найден.")

            log.info("LOGIN ok, token len=%d", len(token))
            return token

    # ---------- CORE ----------

    async def _get(self, path: str):
        try:
            r = await self._c.get(f"{API_DOMAIN}{path}", headers=self._h())
            if r.status_code == 401:
                raise RemangaAuthError("Токен истёк")
            if r.status_code == 429:
                await asyncio.sleep(30)
                return None
            return r.json() if r.status_code == 200 else None
        except httpx.HTTPError:
            return None

    async def _post(self, path: str, body=None):
        try:
            r = await self._c.post(f"{API_DOMAIN}{path}", json=body or {}, headers=self._h())
            if r.status_code == 401:
                raise RemangaAuthError("Токен истёк")
            if r.status_code == 429:
                await asyncio.sleep(30)
                return r.status_code, {"_rate_limited": True}
            try:
                data = r.json()
            except Exception:
                data = {"text": r.text[:200]}
            return r.status_code, data
        except httpx.HTTPError as e:
            return 0, {"_error": str(e)}

    # ---------- PROFILE / BATTLE ----------

    async def profile(self):
        return await self._get("/api/v2/events/card-battle/profile/")

    async def eventpoint_balance(self):
        return await self._get("/api/v2/events/eventpoint-balance/")

    async def raid(self, loc: int):
        return await self._post(f"/api/v2/events/card-battle/locations/{loc}/raid/")

    async def pvp(self):
        return await self._post("/api/v2/events/card-battle/pvp/match/")

    # ---------- CATACOMBS ----------

    async def cata_state(self):
        return await self._get("/api/v2/events/card-battle/catacombs/state/")

    async def cata_levels(self):
        return await self._get("/api/v2/events/card-battle/catacombs/levels/")

    async def cata_enter(self, lvl: int, stars: int):
        return await self._post(
            f"/api/v2/events/card-battle/catacombs/levels/{lvl}/enter/",
            {"stars": stars},
        )

    async def cata_resolve(self, attempt_id):
        return await self._post(
            f"/api/v2/events/card-battle/catacombs/mini-game/{attempt_id}/resolve/",
            {"outcome": "won", "proof": {}},
        )

    # ---------- SHOP ----------

    async def buy_shop_item(self, item_id: int, amount: int = 1):
        if item_id in FORBIDDEN_SHOP_IDS:
            log.error("BLOCKED: попытка купить запрещённый товар id=%s", item_id)
            return 403, {"error": "forbidden"}
        return await self._post(f"/api/v2/shop/buy/{item_id}/", {"amount": amount})


def raid_energy(loc: int) -> int:
    return RAID_ENERGY_COST.get(loc, 13)


# ============================================================
# 5. АВТОБОЙ
# ============================================================

@dataclass
class Flags:
    raid: bool = True
    pvp: bool = True
    cata: bool = True
    raid_loc: int = 10
    cata_fixed: bool = True
    cata_level: int = 17
    cata_stars: int = 1


class AutoBattler:
    def __init__(self, chat_id: int, token: str, notify, flags: Flags):
        self.chat_id = chat_id
        self.token = token
        self.notify = notify
        self.flags = flags
        self.task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _emit(self, text: str, important: bool = False):
        log.info("[chat=%s] %s", self.chat_id, text)
        if important:
            with suppress(Exception):
                await self.notify(text)

    async def start(self):
        if self.task and not self.task.done():
            return
        self._stop.clear()
        self.task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None

    # ---------- CATACOMBS: ЛИМИТ RESURRECT ----------

    async def _resurrect_done(self, cli: RemangaClient) -> bool:
        """
        True, если по свитку 'resurrect' достигнут дневной лимит.
        Если данных нет — False (значит, ещё идём).
        """
        state = await cli.cata_state() or {}
        for s in state.get("scrolls") or []:
            if not isinstance(s, dict):
                continue
            if s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0)
                if limit <= 0:
                    limit = CATA_RESURRECT_LIMIT
                return used >= limit
        return False

    def _resurrect_line(self, state: dict) -> str:
        for s in state.get("scrolls") or []:
            if not isinstance(s, dict):
                continue
            if s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0)
                qty = int(s.get("quantity", 0) or 0)
                mark = "✅" if used >= limit else "⏳"
                return f"{mark} Воскрешение: {used}/{limit} (в наличии {qty})"
        return "⏳ Воскрешение: нет данных"

    # ---------- MAIN LOOP ----------

    async def _run(self):
        cli = RemangaClient(self.token)
        last_pvp = last_raid = last_cata = 0
        last_status = 0
        last_pvp_cooldown = 0

        cata_desc = (
            f"{self.flags.cata_level}/{self.flags.cata_stars}★"
            if self.flags.cata_fixed else "авто (первый непройденный)"
        )
        await self._emit(
            f"🟢 Автобой запущен\n"
            f"⚔️ Рейд: {'вкл' if self.flags.raid else 'выкл'} "
            f"(локация {self.flags.raid_loc})\n"
            f"🏆 PvP: {'вкл' if self.flags.pvp else 'выкл'}\n"
            f"🏺 Катакомбы: {'вкл' if self.flags.cata else 'выкл'} — {cata_desc}\n"
            f"📜 Охота: только Воскрешение (лимит 3/день)\n"
            f"🕒 Окно: 03:00–23:00 МСК",
            important=True,
        )

        cata_levels_cache: list = []

        async def get_cata_levels():
            nonlocal cata_levels_cache
            if not cata_levels_cache:
                cata_levels_cache = await cli.cata_levels() or []
            return cata_levels_cache

        try:
            while not self._stop.is_set():
                try:
                    now = time.time()

                    profile = await cli.profile() or {}
                    energy = int(profile.get("energy_current", 0) or 0)
                    cur_stars = int(profile.get("awakening_energy", 0) or 0)

                    # --- сколько энергии нужно на катакомбы ---
                    reserve = 0
                    need_stars_cata = 0
                    if self.flags.cata and self.flags.cata_fixed:
                        levels = await get_cata_levels()
                        lvl_cfg = next(
                            (x for x in levels if x.get("level") == self.flags.cata_level),
                            None,
                        )
                        if lvl_cfg:
                            reserve = int(
                                lvl_cfg.get("energy_cost", {}).get(
                                    str(self.flags.cata_stars), 0
                                ) or 0
                            )
                            need_stars_cata = int(
                                lvl_cfg.get("awakening_energy_cost", {}).get(
                                    str(self.flags.cata_stars), 0
                                ) or 0
                            )

                    # ============================================
                    # 1. PvP — раз в 32 сек, всегда, бесплатно
                    #    В чат не пишем.
                    # ============================================
                    if self.flags.pvp and now - last_pvp >= max(PVP_INTERVAL, last_pvp_cooldown):
                        last_pvp = now
                        code, resp = await cli.pvp()
                        if code == 200:
                            last_pvp_cooldown = int(resp.get("pvp_cooldown_seconds", 0) or 0)
                            winner = resp.get("winner") or (resp.get("battle") or {}).get("winner") or ""
                            await self._emit(f"🏆 PvP: {winner or 'ok'}", important=False)
                        elif code == 0:
                            await self._emit("⚠️ PvP: сетевая ошибка", important=False)
                        else:
                            last_pvp_cooldown = 30

                    # ============================================
                    # 2. Катакомбы — окно МСК, resurrect-лимит,
                    #    энергия и звёзды
                    # ============================================
                    in_window = msk_in_cata_window()
                    resurrect_done = True
                    if self.flags.cata:
                        resurrect_done = await self._resurrect_done(cli)

                    if (
                        self.flags.cata
                        and now - last_cata >= CATACOMB_INTERVAL
                        and in_window
                        and not resurrect_done
                        and energy >= reserve
                        and cur_stars >= need_stars_cata
                    ):
                        last_cata = now
                        ok = await self._do_cata(cli)
                        await asyncio.sleep(LOOP_SLEEP)
                        continue

                    # ============================================
                    # 3. Рейд — только если звёзд меньше потолка
                    #    и после рейда останется резерв на катакомбы
                    # ============================================
                    if (
                        self.flags.raid
                        and cur_stars < MAX_STARS_FOR_RAID
                        and now - last_raid >= RAID_INTERVAL
                    ):
                        need_raid = raid_energy(self.flags.raid_loc)
                        if energy - need_raid >= reserve:
                            last_raid = now
                            await self._do_raid(cli, self.flags.raid_loc)
                        else:
                            await self._emit(
                                f"⚔️ Рейд отложен: энергии {energy}, "
                                f"нужно {need_raid} + {reserve} резерв",
                                important=False,
                            )

                    # ============================================
                    # 4. Отчёт раз в час
                    # ============================================
                    if now - last_status >= 3600:
                        last_status = now
                        dust = int(profile.get("reroll_dust", 0) or 0)
                        rerolls = int(profile.get("full_reroll_energy", 0) or 0)
                        ep = await cli.eventpoint_balance() or {}
                        points = int(ep.get("balance", 0) or 0)

                        state = await cli.cata_state() or {}
                        resurrect_line = self._resurrect_line(state)

                        if not in_window:
                            sec = msk_seconds_to_window_start()
                            hh, mm = sec // 3600, (sec % 3600) // 60
                            cata_status = f"🕒 Вне окна, до 03:00 МСК: {hh}ч {mm}м"
                        elif resurrect_done:
                            cata_status = "✅ Воскрешение добито — ждём 03:00 МСК"
                        else:
                            cata_status = "📜 Идём за Воскрешением"

                        lines = [
                            "📊 Статус",
                            f"⚡ Энергия: {energy}/185",
                            f"⭐ Звёзды: {cur_stars} (потолок фарма {MAX_STARS_FOR_RAID})",
                            f"🎲 Рероллы: {rerolls}",
                            f"💠 Пыль: {dust}",
                            f"🎯 Event points: {points}",
                            cata_status,
                            resurrect_line,
                        ]
                        await self._emit("\n".join(lines), important=True)

                except RemangaAuthError:
                    await self._emit("🔐 Токен истёк. Выполните /login заново.", important=True)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("autobattle error")
                    await self._emit(f"❌ Ошибка: {e}", important=True)

                await asyncio.sleep(LOOP_SLEEP)
        finally:
            await cli.close()

    # ---------- ACTIONS ----------

    async def _do_raid(self, cli: RemangaClient, loc: int):
        code, resp = await cli.raid(loc)
        if code == 200:
            rewards = resp.get("rewards") or []
            rtxt = ", ".join(
                f"{r.get('kind')} x{r.get('amount')}"
                for r in rewards if isinstance(r, dict)
            ) if rewards else "ok"
            await self._emit(f"⚔️ Рейд {loc}: {rtxt}", important=False)
        else:
            err = resp.get("detail") or resp.get("error") or resp
            await self._emit(f"⚠️ Рейд {loc}: HTTP {code} — {err}", important=False)

    async def _do_cata(self, cli: RemangaClient):
        state = await cli.cata_state() or {}
        profile = await cli.profile() or {}

        # дорешаем висящую мини-игру
        pending = state.get("mini_game") or (state.get("current_run") or {}).get("mini_game")
        pending_id = (pending or {}).get("attempt_id") or state.get("attempt_id")
        if pending_id:
            await cli.cata_resolve(pending_id)
            await self._emit("🏺 Дорешана незакрытая мини-игра", important=False)

        if self.flags.cata_fixed:
            lvl, stars = self.flags.cata_level, self.flags.cata_stars
        else:
            cleared = state.get("cleared_stars") or {}
            lvl = stars = None
            for i in range(1, 26):
                s = int(cleared.get(str(i), 0) or 0)
                if s < 5:
                    lvl, stars = i, s + 1
                    break
        if not lvl:
            return

        levels = await cli.cata_levels() or []
        lvl_cfg = next((x for x in levels if x.get("level") == lvl), None)
        if not lvl_cfg:
            return

        need_energy  = int(lvl_cfg.get("energy_cost", {}).get(str(stars), 0) or 0)
        need_stars   = int(lvl_cfg.get("awakening_energy_cost", {}).get(str(stars), 0) or 0)
        need_rerolls = int(lvl_cfg.get("full_reroll_energy_cost", {}).get(str(stars), 0) or 0)

        cur_energy  = int(profile.get("energy_current", 0) or 0)
        cur_stars   = int(profile.get("awakening_energy", 0) or 0)
        cur_rerolls = int(profile.get("full_reroll_energy", 0) or 0)

        if cur_energy < need_energy:
            await self._emit(
                f"⚡ Энергии мало для {lvl}/{stars}★: {cur_energy}/{need_energy}",
                important=False,
            )
            return

        if cur_stars < need_stars:
            await self._emit(
                f"⭐ Звёзд мало для {lvl}/{stars}★: {cur_stars}/{need_stars}. "
                f"Иду фармить рейд.",
                important=False,
            )
            await self._do_raid(cli, self.flags.raid_loc)
            return

        if cur_rerolls < need_rerolls:
            await self._emit(
                f"🎲 Рероллов мало для {lvl}/{stars}★: {cur_rerolls}/{need_rerolls}",
                important=False,
            )
            return

        code, resp = await cli.cata_enter(lvl, stars)
        if code != 200:
            err = resp.get("detail") or resp.get("error") or resp
            await self._emit(f"🏺 Катакомбы {lvl}/{stars}★: ошибка — {err}", important=True)
            return

        kind = resp.get("kind")
        reward_text = ""
        if kind == "mini_game_required":
            aid = (resp.get("mini_game") or {}).get("attempt_id") or resp.get("attempt_id")
            if aid:
                await cli.cata_resolve(aid)
                reward_text = "мини-игра пройдена"
            else:
                reward_text = "мини-игра без ID"
        elif kind == "run_finished":
            rewards = resp.get("rewards") or []
            reward_text = ", ".join(
                f"{r.get('kind')} x{r.get('amount')}"
                for r in rewards if isinstance(r, dict)
            ) or "ok"
        else:
            reward_text = str(kind or "ok")

        # Обновим state, чтобы увидеть актуальный daily_used
        new_state = await cli.cata_state() or {}
        resurrect_line = self._resurrect_line(new_state)

        await self._emit(
            f"🏺 Катакомбы {lvl}/{stars}★: {reward_text}\n{resurrect_line}",
            important=True,
        )


_battlers: dict[int, AutoBattler] = {}


def get_battler(chat_id: int) -> AutoBattler | None:
    return _battlers.get(chat_id)


async def register_battler(chat_id: int, token: str, notify, flags: Flags) -> AutoBattler:
    old = _battlers.get(chat_id)
    if old:
        await old.stop()
    b = AutoBattler(chat_id, token, notify, flags)
    _battlers[chat_id] = b
    await b.start()
    return b


async def stop_battler(chat_id: int):
    b = _battlers.pop(chat_id, None)
    if b:
        await b.stop()


# ============================================================
# 6. TELEGRAM-ХЕНДЛЕРЫ
# ============================================================

router = Router()


def main_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="▶️ Старт",     callback_data="ctl:start")
    kb.button(text="⏹ Стоп",      callback_data="ctl:stop")
    kb.button(text="⚙️ Настройки", callback_data="ctl:settings")
    kb.button(text="ℹ️ Профиль",   callback_data="ctl:profile")
    kb.button(text="🚪 Выйти",     callback_data="ctl:logout")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def settings_kb(u: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{'✅' if u['raid_enabled'] else '❌'} Рейд",
              callback_data="tgl:raid_enabled")
    kb.button(text=f"{'✅' if u['pvp_enabled']  else '❌'} PvP",
              callback_data="tgl:pvp_enabled")
    kb.button(text=f"{'✅' if u['cata_enabled'] else '❌'} Катакомбы",
              callback_data="tgl:cata_enabled")
    kb.button(text=f"📍 Рейд-локация: {u['raid_loc']}",
              callback_data="raid:loc")

    cata_desc = (
        f"🎯 Катакомбы: {u['cata_level']}/{u['cata_stars']}★"
        if u["cata_fixed"] else "🎯 Катакомбы: авто"
    )
    kb.button(text=cata_desc, callback_data="cata:menu")
    kb.button(text="⬅️ Назад", callback_data="ctl:menu")
    kb.adjust(1, 1, 1, 1, 1, 1)
    return kb.as_markup()


def raid_loc_kb(current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for loc in range(1, 11):
        kb.button(
            text=("• " if loc == current else "") + f"Локация {loc}",
            callback_data=f"raid:set:{loc}",
        )
    kb.button(text="⬅️ Назад", callback_data="ctl:settings")
    kb.adjust(3, 3, 3, 1)
    return kb.as_markup()


def cata_menu_kb(u: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"{'✅' if not u['cata_fixed'] else '▫️'} Авто (первый непройденный)",
        callback_data="cata:mode:auto",
    )
    kb.button(
        text=f"{'✅' if u['cata_fixed'] else '▫️'} Фикс. ярус и сложность",
        callback_data="cata:mode:fixed",
    )
    kb.button(text=f"📍 Уровень: {u['cata_level']}",   callback_data="cata:choose_level")
    kb.button(text=f"⭐ Сложность: {u['cata_stars']}★", callback_data="cata:choose_stars")
    kb.button(text="⬅️ Назад", callback_data="ctl:settings")
    kb.adjust(1, 1, 1, 1, 1)
    return kb.as_markup()


def cata_level_kb(current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for lvl in range(1, 26):
        mark = "• " if lvl == current else ""
        kb.button(text=f"{mark}{lvl}", callback_data=f"cata:setlvl:{lvl}")
    kb.button(text="⬅️ Назад", callback_data="cata:menu")
    kb.adjust(5, 5, 5, 5, 5, 1)
    return kb.as_markup()


def cata_stars_kb(current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for s in range(1, 6):
        mark = "• " if s == current else ""
        kb.button(text=f"{mark}{s}★", callback_data=f"cata:setstars:{s}")
    kb.button(text="⬅️ Назад", callback_data="cata:menu")
    kb.adjust(5, 1)
    return kb.as_markup()


@router.message(Command("start"))
async def cmd_start(m: Message):
    u = await get_user(m.chat.id)
    if u:
        await m.answer("👋 Аккаунт привязан.\nУправление:", reply_markup=main_kb())
    else:
        await m.answer(
            "👋 <b>Remanga AutoBattle Bot</b>\n\n"
            "Привяжите аккаунт:\n"
            "<code>/login логин пароль</code>"
        )


@router.message(Command("login"))
async def cmd_login(m: Message, command: CommandObject):
    if not command.args:
        await m.answer("Формат: <code>/login логин пароль</code>")
        return
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("Формат: <code>/login логин пароль</code>")
        return
    login, password = parts[0], parts[1]

    msg = await m.answer("🔐 Проверяю данные...")
    try:
        token = await RemangaClient.login(login, password)
    except RemangaAuthError as e:
        await msg.edit_text(f"❌ Не удалось войти: {e}")
        return
    except Exception:
        await msg.edit_text("❌ Ошибка соединения.")
        return

    await save_user(m.chat.id, login, password)
    await save_token(m.chat.id, token)

    with suppress(Exception):
        await m.delete()  # удалить сообщение с паролем

    await msg.edit_text(
        "✅ Аккаунт привязан (пароль зашифрован).\nУправление:",
        reply_markup=main_kb(),
    )


@router.message(Command("logout"))
async def cmd_logout(m: Message):
    await stop_battler(m.chat.id)
    await delete_user(m.chat.id)
    await m.answer("🚪 Аккаунт удалён, автобой остановлен.")


@router.callback_query(F.data == "ctl:menu")
async def cb_menu(cq: CallbackQuery):
    await cq.message.edit_text("Управление:", reply_markup=main_kb())
    await cq.answer()


@router.callback_query(F.data == "ctl:start")
async def cb_start(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u or not u["token"]:
        await cq.answer("Сначала /login", show_alert=True)
        return
    if get_battler(cq.from_user.id):
        await cq.answer("Уже запущено", show_alert=True)
        return

    flags = Flags(
        raid=u["raid_enabled"],
        pvp=u["pvp_enabled"],
        cata=u["cata_enabled"],
        raid_loc=u["raid_loc"],
        cata_fixed=u["cata_fixed"],
        cata_level=u["cata_level"],
        cata_stars=u["cata_stars"],
    )

    async def notify(text: str):
        with suppress(Exception):
            await cq.bot.send_message(cq.from_user.id, text)

    await register_battler(cq.from_user.id, u["token"], notify, flags)
    await cq.message.edit_text("✅ Автобой запущен.", reply_markup=main_kb())
    await cq.answer()


@router.callback_query(F.data == "ctl:stop")
async def cb_stop(cq: CallbackQuery):
    await stop_battler(cq.from_user.id)
    await cq.message.edit_text("⏹ Автобой остановлен.", reply_markup=main_kb())
    await cq.answer()


@router.callback_query(F.data == "ctl:profile")
async def cb_profile(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u or not u["token"]:
        await cq.answer("Нет данных", show_alert=True)
        return
    cli = RemangaClient(u["token"])
    try:
        p = await cli.profile() or {}
        ep = await cli.eventpoint_balance() or {}
        state = await cli.cata_state() or {}
    finally:
        await cli.close()

    text = (
        "👤 <b>Профиль</b>\n"
        f"⚡ Энергия: {p.get('energy_current', '?')}/{p.get('energy_max', '?')}\n"
        f"⭐ Звёзды: {p.get('awakening_energy', '?')}\n"
        f"🎲 Рероллы: {p.get('full_reroll_energy', '?')}\n"
        f"💠 Пыль: {p.get('reroll_dust', '?')}\n"
        f"🎯 Event points: {ep.get('balance', '?')}\n"
        f"🏆 PvP: {p.get('pvp_wins', 0)}W / {p.get('pvp_losses', 0)}L\n\n"
        f"{AutoBattler._resurrect_line(None, state)}"
    )
    await cq.message.edit_text(text, reply_markup=main_kb())
    await cq.answer()


@router.callback_query(F.data == "ctl:settings")
async def cb_settings(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Сначала /login", show_alert=True)
        return
    await cq.message.edit_text("⚙️ Настройки:", reply_markup=settings_kb(u))
    await cq.answer()


@router.callback_query(F.data.startswith("tgl:"))
async def cb_toggle(cq: CallbackQuery):
    field = cq.data.split(":", 1)[1]
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Нет данных", show_alert=True)
        return
    new_val = 0 if u[field] else 1
    await update_flag(cq.from_user.id, field, new_val)

    if get_battler(cq.from_user.id):
        await stop_battler(cq.from_user.id)

    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("⚙️ Настройки:", reply_markup=settings_kb(u))
    await cq.answer("Обновлено")


@router.callback_query(F.data == "raid:loc")
async def cb_raid_loc(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Нет данных", show_alert=True)
        return
    await cq.message.edit_text(
        "📍 Выберите локацию рейда:", reply_markup=raid_loc_kb(u["raid_loc"])
    )
    await cq.answer()


@router.callback_query(F.data.startswith("raid:set:"))
async def cb_raid_set(cq: CallbackQuery):
    loc = int(cq.data.split(":")[2])
    await update_flag(cq.from_user.id, "raid_loc", loc)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("⚙️ Настройки:", reply_markup=settings_kb(u))
    await cq.answer(f"Локация {loc}")


@router.callback_query(F.data == "cata:menu")
async def cb_cata_menu(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Нет данных", show_alert=True)
        return
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(u))
    await cq.answer()


@router.callback_query(F.data == "cata:mode:auto")
async def cb_cata_mode_auto(cq: CallbackQuery):
    await update_flag(cq.from_user.id, "cata_fixed", 0)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(u))
    await cq.answer("Режим: авто")


@router.callback_query(F.data == "cata:mode:fixed")
async def cb_cata_mode_fixed(cq: CallbackQuery):
    await update_flag(cq.from_user.id, "cata_fixed", 1)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(u))
    await cq.answer("Режим: фикс.")


@router.callback_query(F.data == "cata:choose_level")
async def cb_cata_choose_level(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Нет данных", show_alert=True)
        return
    await cq.message.edit_text(
        "📍 Выберите уровень (1–25):", reply_markup=cata_level_kb(u["cata_level"])
    )
    await cq.answer()


@router.callback_query(F.data.startswith("cata:setlvl:"))
async def cb_cata_setlvl(cq: CallbackQuery):
    lvl = int(cq.data.split(":")[2])
    await update_flag(cq.from_user.id, "cata_level", lvl)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(u))
    await cq.answer(f"Уровень {lvl}")


@router.callback_query(F.data == "cata:choose_stars")
async def cb_cata_choose_stars(cq: CallbackQuery):
    u = await get_user(cq.from_user.id)
    if not u:
        await cq.answer("Нет данных", show_alert=True)
        return
    await cq.message.edit_text(
        "⭐ Выберите сложность:", reply_markup=cata_stars_kb(u["cata_stars"])
    )
    await cq.answer()


@router.callback_query(F.data.startswith("cata:setstars:"))
async def cb_cata_setstars(cq: CallbackQuery):
    s = int(cq.data.split(":")[2])
    await update_flag(cq.from_user.id, "cata_stars", s)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(u))
    await cq.answer(f"Сложность {s}★")


@router.callback_query(F.data == "ctl:logout")
async def cb_logout(cq: CallbackQuery):
    await stop_battler(cq.from_user.id)
    await delete_user(cq.from_user.id)
    await cq.message.edit_text("🚪 Аккаунт удалён.")
    await cq.answer()


# ============================================================
# 7. HEALTH-СЕРВЕР
# ============================================================

async def _health_handler(_request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", _health_handler)
    app.router.add_get("/health", _health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health endpoint on :%d", PORT)
    return runner


# ============================================================
# 8. ЗАПУСК
# ============================================================

def make_bot() -> Bot:
    connector = TCPConnector(family=socket.AF_INET)  # только IPv4
    session = AiohttpSession()
    session._connector = connector
    return Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
        timeout=120,
    )


async def main():
    await init_db()
    health_runner = await start_health_server()

    bot = make_bot()
    dp = Dispatcher()
    dp.include_router(router)

    log.info("Bot polling started")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for cid in await all_chat_ids():
            await stop_battler(cid)
        await bot.session.close()
        with suppress(Exception):
            await health_runner.cleanup()
        log.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
