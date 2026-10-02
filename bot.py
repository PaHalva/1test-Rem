# ============================================================
# Remanga AutoBattle Telegram Bot — single file
# ============================================================
# Установка:
#   pip install -r requirements.txt
# Локально:
#   .env с BOT_TOKEN, FERNET_KEY, DATABASE_URL
#   python bot.py
# На PaaS:
#   ENV: BOT_TOKEN, FERNET_KEY, PORT, DATABASE_URL
# ============================================================

import asyncio
import logging
import os
import random
import socket
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg
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
# 1. ЛОГИРОВАНИЕ
# ============================================================

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

load_dotenv(override=False)

BOT_TOKEN    = os.environ.get("BOT_TOKEN", "").strip()
FERNET_KEY   = os.environ.get("FERNET_KEY", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PORT         = int(os.environ.get("PORT", "8080"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан")
if not FERNET_KEY:
    raise RuntimeError("FERNET_KEY не задан")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL не задан")

try:
    fernet = Fernet(FERNET_KEY.encode())
except Exception as e:
    raise RuntimeError(f"FERNET_KEY некорректен: {e}")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]

for junk in (
    "?sslmode=require", "&sslmode=require",
    "?ssl=true",        "&ssl=true",
    "?channel_binding=require", "&channel_binding=require",
):
    DATABASE_URL = DATABASE_URL.replace(junk, "")

API_DOMAIN = "https://api.remanga.org"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)


# ============================================================
# 3. УСТОЙЧИВОСТЬ И ГЛОБАЛЬНЫЕ ОГРАНИЧЕНИЯ
# ============================================================

# Одновременных запросов к Remanga.
REMANGA_CONCURRENCY       = 40
REMANGA_CONCURRENCY_SLOW  = 15

# PvP: базовый интервал 32 сек + jitter 0..3; при 429 → 60 сек.
PVP_INTERVAL           = 32
PVP_JITTER             = 3
PVP_INTERVAL_THROTTLED = 60

# Авто-торможение при 429.
RATE_LIMIT_WINDOW    = 60
RATE_LIMIT_THRESHOLD = 5

# Интервалы основного цикла.
RAID_INTERVAL             = 300
STATUS_INTERVAL           = 3600
BATTLE_LOOP_INTERVAL      = 600   # 10 минут — когда катакомбы открыты
BATTLE_LOOP_INTERVAL_IDLE = 900   # 15 минут — когда катакомбы закрыты
SERVER_DOWN_RETRY         = 300   # 5 минут — если Remanga лежит
START_JITTER              = 30    # jitter при старте автобоя
MINI_GAME_MAX_ATTEMPTS    = 3

RAID_ENERGY_COST = {1: 4, 2: 5, 3: 6, 4: 7, 5: 8, 6: 9, 7: 10, 8: 11, 9: 12, 10: 13}

# ------------------------------------------------------------
# МАГАЗИН
# ------------------------------------------------------------
SHOP_AWAKENING_ENERGY_ID = 6333   # 35 points = 1★
SHOP_COST_EVENT_POINTS   = 35
FORBIDDEN_SHOP_IDS = {6332}       # restore-energy — НИКОГДА

# Потолок автопокупки звёзд.
MAX_STARS_AUTO_BUY = 10

# ------------------------------------------------------------
# КАТАКОМБЫ: ОКНО И ЛИМИТ (МСК)
# ------------------------------------------------------------
MSK = timezone(timedelta(hours=3))
CATA_WINDOW_START_HOUR = 3
CATA_WINDOW_END_HOUR   = 23
CATA_RESURRECT_LIMIT   = 3


def msk_now() -> datetime:
    return datetime.now(MSK)


def msk_in_cata_window() -> bool:
    h = msk_now().hour
    return CATA_WINDOW_START_HOUR <= h < CATA_WINDOW_END_HOUR


def msk_seconds_to_window_start() -> int:
    now = msk_now()
    target = now.replace(hour=CATA_WINDOW_START_HOUR, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return int((target - now).total_seconds())


# ------------------------------------------------------------
# Динамический семафор
# ------------------------------------------------------------

class DynamicSemaphore:
    """Семафор с возможностью менять лимит на лету."""
    def __init__(self, limit: int):
        self._limit = limit
        self._current = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    async def set_limit(self, new_limit: int):
        async with self._cond:
            self._limit = max(1, new_limit)
            self._cond.notify_all()

    async def __aenter__(self):
        async with self._cond:
            while self._current >= self._limit:
                await self._cond.wait()
            self._current += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        async with self._cond:
            self._current -= 1
            self._cond.notify()


_remanga_sem = DynamicSemaphore(REMANGA_CONCURRENCY)

# --- Трекер 429 ---
_rate_limit_times: list[float] = []
_rate_limit_throttled_until: float = 0.0


def _record_429():
    now = time.time()
    _rate_limit_times.append(now)
    cutoff = now - RATE_LIMIT_WINDOW
    while _rate_limit_times and _rate_limit_times[0] < cutoff:
        _rate_limit_times.pop(0)


def _is_throttled() -> bool:
    return time.time() < _rate_limit_throttled_until


async def _maybe_throttle_or_unthrottle():
    global _rate_limit_throttled_until
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW
    recent = [t for t in _rate_limit_times if t >= cutoff]

    if len(recent) >= RATE_LIMIT_THRESHOLD and not _is_throttled():
        _rate_limit_throttled_until = now + 300
        await _remanga_sem.set_limit(REMANGA_CONCURRENCY_SLOW)
        log.warning(
            "Rate limit detected (%d 429 in %ds). Throttling for 5 min: "
            "concurrency=%d, PVP_INTERVAL=%d",
            len(recent), RATE_LIMIT_WINDOW,
            REMANGA_CONCURRENCY_SLOW, PVP_INTERVAL_THROTTLED,
        )
    elif _is_throttled() and now >= _rate_limit_throttled_until:
        _rate_limit_throttled_until = 0
        await _remanga_sem.set_limit(REMANGA_CONCURRENCY)
        _rate_limit_times.clear()
        log.info(
            "Rate limit recovered. Restored: concurrency=%d, PVP_INTERVAL=%d",
            REMANGA_CONCURRENCY, PVP_INTERVAL,
        )


async def _rate_limit_watcher():
    while True:
        try:
            await _maybe_throttle_or_unthrottle()
        except Exception:
            log.exception("rate limit watcher error")
        await asyncio.sleep(15)


# ============================================================
# 4. БАЗА ДАННЫХ (PostgreSQL через asyncpg + PgBouncer)
# ============================================================

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            ssl=False,
            command_timeout=30,
            statement_cache_size=0,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id       BIGINT PRIMARY KEY,
    login_enc     BYTEA NOT NULL,
    password_enc  BYTEA NOT NULL,
    token_enc     BYTEA,
    raid_enabled  INTEGER DEFAULT 1,
    pvp_enabled   INTEGER DEFAULT 1,
    cata_enabled  INTEGER DEFAULT 1,
    raid_loc      INTEGER DEFAULT 10,
    cata_fixed    INTEGER DEFAULT 1,
    cata_level    INTEGER DEFAULT 17,
    cata_stars    INTEGER DEFAULT 1,
    created_at    BIGINT  DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);
"""

MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS cata_fixed INTEGER DEFAULT 1",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS cata_level INTEGER DEFAULT 17",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS cata_stars INTEGER DEFAULT 1",
]


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(INIT_SQL)
        for sql in MIGRATIONS:
            try:
                await conn.execute(sql)
            except Exception:
                pass
    safe_host = DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "?"
    log.info("DB ready (Postgres @ %s)", safe_host)


async def save_user(chat_id: int, login: str, password: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (chat_id, login_enc, password_enc)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE SET
              login_enc    = EXCLUDED.login_enc,
              password_enc = EXCLUDED.password_enc
            """,
            chat_id,
            fernet.encrypt(login.encode()),
            fernet.encrypt(password.encode()),
        )


async def save_token(chat_id: int, token: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET token_enc=$1 WHERE chat_id=$2",
            fernet.encrypt(token.encode()), chat_id,
        )


async def get_user(chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE chat_id=$1", chat_id)
    if not row:
        return None

    d = dict(row)
    try:
        login = fernet.decrypt(d["login_enc"]).decode()
        password = fernet.decrypt(d["password_enc"]).decode()
        token = fernet.decrypt(d["token_enc"]).decode() if d.get("token_enc") else None
    except Exception as e:
        log.warning("Bad user record chat_id=%s, removing: %s", chat_id, e)
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE chat_id=$1", chat_id)
        return None

    return {
        "chat_id":      d["chat_id"],
        "login":        login,
        "password":     password,
        "token":        token,
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
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE users SET {field}=$1 WHERE chat_id=$2",
            value, chat_id,
        )


async def delete_user(chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE chat_id=$1", chat_id)


async def all_chat_ids():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM users")
    return [r["chat_id"] for r in rows]


# ============================================================
# 5. КЛИЕНТ REMANGA
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

    async def _get(self, path: str, retries: int = 2):
        async with _remanga_sem:
            for attempt in range(retries + 1):
                try:
                    r = await self._c.get(
                        f"{API_DOMAIN}{path}", headers=self._h()
                    )
                    if r.status_code == 401:
                        raise RemangaAuthError("Токен истёк")
                    if r.status_code == 429:
                        _record_429()
                        await asyncio.sleep(30)
                        continue
                    if 500 <= r.status_code < 600:
                        log.warning("Remanga 5xx: %s %s", r.status_code, path)
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return {"_server_down": True}
                    return r.json() if r.status_code == 200 else None
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    log.warning("Remanga net error: %s (%s)", e, path)
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return {"_server_down": True}
            return {"_server_down": True}

    async def _post(self, path: str, body=None, retries: int = 2):
        async with _remanga_sem:
            for attempt in range(retries + 1):
                try:
                    r = await self._c.post(
                        f"{API_DOMAIN}{path}",
                        json=body or {},
                        headers=self._h(),
                    )
                    if r.status_code == 401:
                        raise RemangaAuthError("Токен истёк")
                    if r.status_code == 429:
                        _record_429()
                        await asyncio.sleep(30)
                        continue
                    if 500 <= r.status_code < 600:
                        log.warning("Remanga 5xx POST: %s %s", r.status_code, path)
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return 0, {"_server_down": True}
                    try:
                        data = r.json()
                    except Exception:
                        data = {"text": r.text[:200]}
                    return r.status_code, data
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    log.warning("Remanga net error POST: %s (%s)", e, path)
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return 0, {"_server_down": True}
            return 0, {"_server_down": True}

    async def profile(self):
        return await self._get("/api/v2/events/card-battle/profile/")

    async def eventpoint_balance(self):
        return await self._get("/api/v2/events/eventpoint-balance/")

    async def raid(self, loc: int):
        return await self._post(f"/api/v2/events/card-battle/locations/{loc}/raid/")

    async def pvp(self):
        return await self._post("/api/v2/events/card-battle/pvp/match/")

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

    async def buy_shop_item(self, item_id: int, amount: int = 1):
        if item_id in FORBIDDEN_SHOP_IDS:
            log.error("BLOCKED: попытка купить запрещённый товар id=%s", item_id)
            return 403, {"error": "forbidden"}
        return await self._post(f"/api/v2/shop/buy/{item_id}/", {"amount": amount})


def raid_energy(loc: int) -> int:
    return RAID_ENERGY_COST.get(loc, 13)


# ============================================================
# 6. АВТОБОЙ
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
        self.pvp_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._mini_attempts: dict[int, int] = {}

    async def _emit(self, text: str, important: bool = False):
        log.info("[chat=%s] %s", self.chat_id, text)
        if important:
            with suppress(Exception):
                await self.notify(text)

    async def start(self):
        if self.task and not self.task.done():
            return
        self._stop.clear()

        jitter = random.uniform(0, START_JITTER)

        async def _delayed_start():
            await asyncio.sleep(jitter)
            await self._run_safe()

        self.task = asyncio.create_task(_delayed_start())
        if self.flags.pvp:
            self.pvp_task = asyncio.create_task(self._pvp_loop())

    async def stop(self):
        self._stop.set()
        for t in (self.task, self.pvp_task):
            if t:
                t.cancel()
                with suppress(asyncio.CancelledError):
                    await t
        self.task = None
        self.pvp_task = None
        self._mini_attempts.clear()

    async def _run_safe(self):
        """Обёртка: перезапускает _run при падении."""
        while not self._stop.is_set():
            try:
                await self._run()
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("autobattle crashed chat_id=%s", self.chat_id)
                with suppress(Exception):
                    await self.notify(f"⚠️ Цикл перезапустится через 30 сек: {e}")
                await asyncio.sleep(30)

    # ---------- PvP ----------

    async def _pvp_loop(self):
        cli = RemangaClient(self.token)
        cooldown = 0
        try:
            while not self._stop.is_set():
                try:
                    if not self.flags.pvp:
                        await asyncio.sleep(5)
                        continue

                    code, resp = await cli.pvp()
                    if code == 200:
                        cooldown = int(resp.get("pvp_cooldown_seconds", 0) or 0)
                        winner = resp.get("winner") or (resp.get("battle") or {}).get("winner") or ""
                        await self._emit(f"🏆 PvP: {winner or 'ok'}", important=False)
                    elif code == 0:
                        await self._emit("⚠️ PvP: сетевая ошибка", important=False)
                        cooldown = 30
                    else:
                        cooldown = 30

                    base = PVP_INTERVAL_THROTTLED if _is_throttled() else PVP_INTERVAL
                    jitter = random.uniform(0, PVP_JITTER)
                    await asyncio.sleep(max(base, cooldown) + jitter)

                except RemangaAuthError:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("pvp loop error chat=%s", self.chat_id)
                    await asyncio.sleep(60)
        finally:
            await cli.close()

    # ---------- СВИТКИ ----------

    async def _resurrect_done(self, cli: RemangaClient) -> bool:
        state = await cli.cata_state() or {}
        for s in state.get("scrolls") or []:
            if isinstance(s, dict) and s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0) or CATA_RESURRECT_LIMIT
                return used >= limit
        return False

    @staticmethod
    def _resurrect_line_from_state(state: dict) -> str:
        for s in (state or {}).get("scrolls") or []:
            if isinstance(s, dict) and s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0)
                qty = int(s.get("quantity", 0) or 0)
                mark = "✅" if used >= limit else "⏳"
                return f"{mark} Воскрешение: {used}/{limit} (в наличии {qty})"
        return "⏳ Воскрешение: нет данных"

    # ---------- АВТОПОКУПКА ЗВЁЗД ----------

    async def _ensure_stars(self, cli: RemangaClient, need_stars: int) -> bool:
        profile = await cli.profile()
        if isinstance(profile, dict) and profile.get("_server_down"):
            return False
        profile = profile or {}
        cur_stars = int(profile.get("awakening_energy", 0) or 0)
        if cur_stars >= need_stars:
            return True

        if cur_stars >= MAX_STARS_AUTO_BUY:
            await self._emit(
                f"⛔ Звёзд {cur_stars} ≥ {MAX_STARS_AUTO_BUY} — автопокупка отключена",
                important=False,
            )
            return False

        missing = need_stars - cur_stars
        max_buyable = MAX_STARS_AUTO_BUY - cur_stars
        buy_count = min(missing, max_buyable)
        if buy_count <= 0:
            return False

        ep = await cli.eventpoint_balance()
        if isinstance(ep, dict) and ep.get("_server_down"):
            return False
        ep = ep or {}
        cur_points = int(ep.get("balance", 0) or 0)
        need_points = buy_count * SHOP_COST_EVENT_POINTS

        if cur_points < need_points:
            await self._emit(
                f"⭐ Звёзд {cur_stars}/{need_stars}, points "
                f"{cur_points}/{need_points}. Фармим рейд.",
                important=False,
            )
            return False

        bought = 0
        for _ in range(buy_count):
            profile_now = await cli.profile() or {}
            if isinstance(profile_now, dict) and profile_now.get("_server_down"):
                break
            stars_now = int(profile_now.get("awakening_energy", 0) or 0)
            if stars_now >= MAX_STARS_AUTO_BUY:
                break

            code, resp = await cli.buy_shop_item(SHOP_AWAKENING_ENERGY_ID, 1)
            if code != 200:
                err = resp.get("detail") or resp.get("error") or resp
                await self._emit(f"⚠️ Покупка звезды: HTTP {code} — {err}", important=True)
                break
            bought += 1
            await asyncio.sleep(1.0)

        if bought > 0:
            await self._emit(
                f"⭐ Куплено {bought}★ за {bought * SHOP_COST_EVENT_POINTS} event points",
                important=True,
            )

        profile_after = await cli.profile() or {}
        if isinstance(profile_after, dict) and profile_after.get("_server_down"):
            return False
        return int(profile_after.get("awakening_energy", 0) or 0) >= need_stars

    # ---------- ОСНОВНОЙ ЦИКЛ ----------

    async def _run(self):
        cli = RemangaClient(self.token)
        last_raid = 0
        last_status = 0

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
            f"⭐ Автопокупка звёзд: до {MAX_STARS_AUTO_BUY}\n"
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
                next_sleep = BATTLE_LOOP_INTERVAL
                try:
                    now = time.time()

                    profile = await cli.profile()
                    if isinstance(profile, dict) and profile.get("_server_down"):
                        await self._emit(
                            "🌐 Сервер Remanga недоступен, жду 5 минут...",
                            important=False,
                        )
                        await asyncio.sleep(SERVER_DOWN_RETRY)
                        continue

                    profile = profile or {}
                    energy = int(profile.get("energy_current", 0) or 0)
                    cur_stars = int(profile.get("awakening_energy", 0) or 0)

                    reserve = 0
                    need_stars_cata = 0
                    if self.flags.cata and self.flags.cata_fixed:
                        levels = await get_cata_levels()
                        if isinstance(levels, dict) and levels.get("_server_down"):
                            await asyncio.sleep(SERVER_DOWN_RETRY)
                            continue
                        lvl_cfg = next(
                            (x for x in (levels or []) if x.get("level") == self.flags.cata_level),
                            None,
                        )
                        if lvl_cfg:
                            reserve = int(
                                lvl_cfg.get("energy_cost", {})
                                .get(str(self.flags.cata_stars), 0) or 0
                            )
                            need_stars_cata = int(
                                lvl_cfg.get("awakening_energy_cost", {})
                                .get(str(self.flags.cata_stars), 0) or 0
                            )

                    in_window = msk_in_cata_window()
                    resurrect_done = True
                    if self.flags.cata:
                        resurrect_done = await self._resurrect_done(cli)

                    cata_open = (
                        self.flags.cata
                        and in_window
                        and not resurrect_done
                    )

                    next_sleep = (
                        BATTLE_LOOP_INTERVAL if cata_open
                        else BATTLE_LOOP_INTERVAL_IDLE
                    )

                    # ---------- 1. Катакомбы ----------
                    stars_ok = True
                    if cata_open and cur_stars < need_stars_cata:
                        stars_ok = await self._ensure_stars(cli, need_stars_cata)
                        profile = await cli.profile() or {}
                        if not isinstance(profile, dict) or not profile.get("_server_down"):
                            cur_stars = int((profile or {}).get("awakening_energy", 0) or 0)

                    if (
                        cata_open
                        and stars_ok
                        and energy >= reserve
                        and cur_stars >= need_stars_cata
                    ):
                        await self._do_cata(cli)
                        await asyncio.sleep(BATTLE_LOOP_INTERVAL)
                        continue

                    # ---------- 2. Рейд ----------
                    cata_ready = (
                        cata_open
                        and energy >= reserve
                        and cur_stars >= need_stars_cata
                    )

                    if (
                        self.flags.raid
                        and not cata_ready
                        and now - last_raid >= RAID_INTERVAL
                    ):
                        last_raid = now
                        need_raid = raid_energy(self.flags.raid_loc)
                        if energy >= need_raid:
                            code, resp = await cli.raid(self.flags.raid_loc)
                            if isinstance(resp, dict) and resp.get("_server_down"):
                                await self._emit(
                                    "🌐 Сервер недоступен (рейд), жду 5 минут...",
                                    important=False,
                                )
                                last_raid = now - RAID_INTERVAL + 60
                                await asyncio.sleep(SERVER_DOWN_RETRY)
                                continue
                            if code == 200:
                                rewards = resp.get("rewards") or []
                                rtxt = ", ".join(
                                    f"{r.get('kind')} x{r.get('amount')}"
                                    for r in rewards if isinstance(r, dict)
                                ) if rewards else "ok"
                                await self._emit(
                                    f"⚔️ Рейд {self.flags.raid_loc}: {rtxt}",
                                    important=False,
                                )

                    # ---------- 3. Отчёт раз в час ----------
                    if now - last_status >= STATUS_INTERVAL:
                        last_status = now
                        dust = int(profile.get("reroll_dust", 0) or 0)
                        rerolls = int(profile.get("full_reroll_energy", 0) or 0)
                        ep = await cli.eventpoint_balance() or {}
                        if isinstance(ep, dict) and ep.get("_server_down"):
                            ep = {}
                        points = int(ep.get("balance", 0) or 0)

                        state = await cli.cata_state() or {}
                        if isinstance(state, dict) and state.get("_server_down"):
                            state = {}
                        resurrect_line = self._resurrect_line_from_state(state)

                        if not in_window:
                            sec = msk_seconds_to_window_start()
                            hh, mm = sec // 3600, (sec % 3600) // 60
                            cata_status = f"🕒 Вне окна, до 03:00 МСК: {hh}ч {mm}м (фарм рейдами)"
                        elif resurrect_done:
                            cata_status = "✅ Воскрешение добито — ждём 03:00 МСК (фарм рейдами)"
                        else:
                            cata_status = "📜 Идём за Воскрешением"

                        if cur_stars >= MAX_STARS_AUTO_BUY:
                            buy_note = f"⛔ автопокупка выкл (≥ {MAX_STARS_AUTO_BUY})"
                        else:
                            buy_note = f"до {MAX_STARS_AUTO_BUY}"

                        lines = [
                            "📊 Статус",
                            f"⚡ Энергия: {energy}/185",
                            f"⭐ Звёзды: {cur_stars} ({buy_note})",
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

                await asyncio.sleep(next_sleep)
        finally:
            await cli.close()

    # ---------- CATACOMBS ----------

    async def _do_cata(self, cli: RemangaClient):
        state = await cli.cata_state()
        if isinstance(state, dict) and state.get("_server_down"):
            await self._emit("🌐 Сервер недоступен (катакомбы), жду", important=False)
            return
        state = state or {}

        profile = await cli.profile()
        if isinstance(profile, dict) and profile.get("_server_down"):
            return
        profile = profile or {}

        # Дорешаем висящую мини-игру с ограничением попыток
        pending = state.get("mini_game") or (state.get("current_run") or {}).get("mini_game")
        pending_id = (pending or {}).get("attempt_id") or state.get("attempt_id")
        if pending_id:
            attempts = self._mini_attempts.get(pending_id, 0)
            if attempts < MINI_GAME_MAX_ATTEMPTS:
                code, resp = await cli.cata_resolve(pending_id)
                if code == 200 and not (isinstance(resp, dict) and resp.get("_server_down")):
                    self._mini_attempts.pop(pending_id, None)
                    await self._emit("🏺 Дорешана незакрытая мини-игра", important=False)
                else:
                    self._mini_attempts[pending_id] = attempts + 1
                    await self._emit(
                        f"⚠️ Мини-игра {pending_id}: попытка {attempts+1}/{MINI_GAME_MAX_ATTEMPTS}",
                        important=False,
                    )
            else:
                await self._emit(
                    f"🚫 Мини-игра {pending_id} не решается после "
                    f"{MINI_GAME_MAX_ATTEMPTS} попыток. Пропускаю.",
                    important=True,
                )
                self._mini_attempts.pop(pending_id, None)
                return

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

        levels = await cli.cata_levels()
        if isinstance(levels, dict) and levels.get("_server_down"):
            return
        lvl_cfg = next((x for x in (levels or []) if x.get("level") == lvl), None)
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
                f"⭐ Звёзд мало для {lvl}/{stars}★: {cur_stars}/{need_stars}",
                important=False,
            )
            return
        if cur_rerolls < need_rerolls:
            await self._emit(
                f"🎲 Рероллов мало для {lvl}/{stars}★: {cur_rerolls}/{need_rerolls}",
                important=False,
            )
            return

        code, resp = await cli.cata_enter(lvl, stars)
        if isinstance(resp, dict) and resp.get("_server_down"):
            return
        if code != 200:
            err = resp.get("detail") or resp.get("error") or resp
            await self._emit(f"🏺 Катакомбы {lvl}/{stars}★: ошибка — {err}", important=True)
            return

        kind = resp.get("kind")
        reward_text = ""
        if kind == "mini_game_required":
            aid = (resp.get("mini_game") or {}).get("attempt_id") or resp.get("attempt_id")
            if aid:
                code2, resp2 = await cli.cata_resolve(aid)
                if code2 == 200 and not (isinstance(resp2, dict) and resp2.get("_server_down")):
                    reward_text = "мини-игра пройдена"
                else:
                    reward_text = "мини-игра не решена"
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

        new_state = await cli.cata_state() or {}
        if isinstance(new_state, dict) and new_state.get("_server_down"):
            new_state = {}
        resurrect_line = self._resurrect_line_from_state(new_state)

        await self._emit(
            f"🏺 Катакомбы {lvl}/{stars}★: {reward_text}\n{resurrect_line}",
            important=True,
        )


# ---------- РЕЕСТР БАТЛЕРОВ ----------

_battlers: dict[int, AutoBattler] = {}
_registry_lock = asyncio.Lock()


def get_battler(chat_id: int) -> AutoBattler | None:
    return _battlers.get(chat_id)


async def register_battler(chat_id: int, token: str, notify, flags: Flags) -> AutoBattler:
    async with _registry_lock:
        old = _battlers.get(chat_id)
        if old:
            await old.stop()
        b = AutoBattler(chat_id, token, notify, flags)
        _battlers[chat_id] = b
        await b.start()
        return b


async def stop_battler(chat_id: int):
    async with _registry_lock:
        b = _battlers.pop(chat_id, None)
    if b:
        await b.stop()


# ============================================================
# 7. TELEGRAM-ХЕНДЛЕРЫ
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
    kb.button(
        text=f"{'✅' if u['raid_enabled'] else '❌'} Рейд",
        callback_data="tgl:raid_enabled",
    )
    kb.button(
        text=f"{'✅' if u['pvp_enabled'] else '❌'} PvP",
        callback_data="tgl:pvp_enabled",
    )
    kb.button(
        text=f"{'✅' if u['cata_enabled'] else '❌'} Катакомбы",
        callback_data="tgl:cata_enabled",
    )
    kb.button(
        text=f"📍 Рейд-локация: {u['raid_loc']}",
        callback_data="raid:loc",
    )
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
    kb.button(
        text=f"📍 Уровень: {u['cata_level']}",
        callback_data="cata:choose_level",
    )
    kb.button(
        text=f"⭐ Сложность: {u['cata_stars']}★",
        callback_data="cata:choose_stars",
    )
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

    try:
        await save_user(m.chat.id, login, password)
        await save_token(m.chat.id, token)
    except Exception as e:
        log.exception("DB error during login")
        await msg.edit_text(f"❌ Ошибка сохранения в БД: {e}")
        return

    with suppress(Exception):
        await m.delete()

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
        if isinstance(p, dict) and p.get("_server_down"):
            p = {}
        ep = await cli.eventpoint_balance() or {}
        if isinstance(ep, dict) and ep.get("_server_down"):
            ep = {}
        state = await cli.cata_state() or {}
        if isinstance(state, dict) and state.get("_server_down"):
            state = {}
    finally:
        await cli.close()

    stars = int(p.get("awakening_energy", 0) or 0)
    if stars >= MAX_STARS_AUTO_BUY:
        buy_note = f"⛔ автопокупка выкл (≥ {MAX_STARS_AUTO_BUY})"
    else:
        buy_note = f"автопокупка до {MAX_STARS_AUTO_BUY}"

    text = (
        "👤 <b>Профиль</b>\n"
        f"⚡ Энергия: {p.get('energy_current', '?')}/{p.get('energy_max', '?')}\n"
        f"⭐ Звёзды: {stars} ({buy_note})\n"
        f"🎲 Рероллы: {p.get('full_reroll_energy', '?')}\n"
        f"💠 Пыль: {p.get('reroll_dust', '?')}\n"
        f"🎯 Event points: {ep.get('balance', '?')}\n"
        f"🏆 PvP: {p.get('pvp_wins', 0)}W / {p.get('pvp_losses', 0)}L\n\n"
        f"{AutoBattler._resurrect_line_from_state(state)}"
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
        u2 = await get_user(cq.from_user.id)

        async def notify(text: str):
            with suppress(Exception):
                await cq.bot.send_message(cq.from_user.id, text)

        flags = Flags(
            raid=u2["raid_enabled"],
            pvp=u2["pvp_enabled"],
            cata=u2["cata_enabled"],
            raid_loc=u2["raid_loc"],
            cata_fixed=u2["cata_fixed"],
            cata_level=u2["cata_level"],
            cata_stars=u2["cata_stars"],
        )
        await register_battler(cq.from_user.id, u2["token"], notify, flags)

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
# 8. HEALTH-СЕРВЕР
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
# 9. ЗАПУСК
# ============================================================

def make_bot() -> Bot:
    connector = TCPConnector(family=socket.AF_INET)
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

    # Наблюдатель за 429 — авто-торможение
    asyncio.create_task(_rate_limit_watcher())

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
        await close_pool()
        log.info("Bot stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
