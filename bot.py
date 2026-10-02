# ============================================================
# Remanga AutoBattle Telegram Bot — single file (IPv4 forced)
# ============================================================
# Локально:
#   pip install -r requirements.txt
#   python bot.py
#
# На PaaS (relaxdev и т.п.):
#   Задайте ENV: BOT_TOKEN, FERNET_KEY, DB_PATH, PORT
# ============================================================

import asyncio
import logging
import os
import socket
import time
from contextlib import suppress
from dataclasses import dataclass
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
from aiohttp import ClientSession, TCPConnector, web
from cryptography.fernet import Fernet
from dotenv import load_dotenv


# ============================================================
# 1. КОНФИГ
# ============================================================

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)
else:
    load_dotenv(override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("remanga-bot")

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


# ============================================================
# 2. БАЗА ДАННЫХ
# ============================================================

INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id       INTEGER PRIMARY KEY,
    login_enc     BLOB NOT NULL,
    password_enc  BLOB NOT NULL,
    token_enc     BLOB,
    raid_enabled  INTEGER DEFAULT 1,
    pvp_enabled   INTEGER DEFAULT 1,
    cata_enabled  INTEGER DEFAULT 1,
    raid_loc      INTEGER DEFAULT 10,
    created_at    INTEGER DEFAULT (strftime('%s','now'))
);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
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
            return {
                "chat_id":      row["chat_id"],
                "login":        fernet.decrypt(row["login_enc"]).decode(),
                "password":     fernet.decrypt(row["password_enc"]).decode(),
                "token":        fernet.decrypt(row["token_enc"]).decode() if row["token_enc"] else None,
                "raid_enabled": bool(row["raid_enabled"]),
                "pvp_enabled":  bool(row["pvp_enabled"]),
                "cata_enabled": bool(row["cata_enabled"]),
                "raid_loc":     row["raid_loc"],
            }


async def update_flag(chat_id: int, field: str, value):
    assert field in ("raid_enabled", "pvp_enabled", "cata_enabled", "raid_loc")
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
# 3. КЛИЕНТ REMANGA
# ============================================================

class RemangaAuthError(Exception):
    pass


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
            if r.status_code == 200:
                data = r.json()
                tok = data.get("token") or data.get("access_token")
                if not tok:
                    raise RemangaAuthError("Сервер вернул 200 без токена.")
                return tok
            if r.status_code in (400, 401, 403):
                raise RemangaAuthError(f"Неверный логин/пароль (HTTP {r.status_code})")
            raise RemangaAuthError(f"Ошибка входа: HTTP {r.status_code}")

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

    async def profile(self):
        return await self._get("/api/v2/events/card-battle/profile/")

    async def raid(self, loc: int):
        return await self._post(f"/api/v2/events/card-battle/locations/{loc}/raid/")

    async def pvp(self):
        return await self._post("/api/v2/events/card-battle/pvp/match/")

    async def cata_state(self):
        return await self._get("/api/v2/events/card-battle/catacombs/state/")

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


def raid_energy(loc: int) -> int:
    return RAID_ENERGY_COST.get(loc, 13)


# ============================================================
# 4. АВТОБОЙ
# ============================================================

@dataclass
class Flags:
    raid: bool = True
    pvp: bool = True
    cata: bool = True
    raid_loc: int = 10


class AutoBattler:
    def __init__(self, chat_id: int, token: str, notify, flags: Flags):
        self.chat_id = chat_id
        self.token = token
        self.notify = notify
        self.flags = flags
        self.task: asyncio.Task | None = None
        self._stop = asyncio.Event()

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

    async def _run(self):
        cli = RemangaClient(self.token)
        last_pvp = last_raid = last_cata = 0
        await self.notify(
            f"🟢 Автобой запущен\n"
            f"⚔️ Рейд: {'вкл' if self.flags.raid else 'выкл'}\n"
            f"🏆 PvP: {'вкл' if self.flags.pvp else 'выкл'}\n"
            f"🏺 Катакомбы: {'вкл' if self.flags.cata else 'выкл'}"
        )
        try:
            while not self._stop.is_set():
                try:
                    prof = await cli.profile()
                    if prof is None:
                        await asyncio.sleep(LOOP_SLEEP * 2)
                        continue

                    energy = int(prof.get("energy_current", 0))
                    now = time.time()

                    if self.flags.pvp and now - last_pvp >= PVP_INTERVAL:
                        last_pvp = now
                        await self._do_pvp(cli)

                    if self.flags.raid and now - last_raid >= RAID_INTERVAL:
                        need = raid_energy(self.flags.raid_loc)
                        if energy >= need:
                            last_raid = now
                            await self._do_raid(cli, self.flags.raid_loc)
                        else:
                            last_raid = now - RAID_INTERVAL + 30

                    if self.flags.cata and now - last_cata >= CATACOMB_INTERVAL:
                        last_cata = now
                        await self._do_cata(cli)

                except RemangaAuthError:
                    await self.notify("🔐 Токен истёк. Выполните /login заново.")
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.exception("autobattle error")
                    await self.notify(f"❌ Ошибка: {e}")

                await asyncio.sleep(LOOP_SLEEP)
        finally:
            await cli.close()

    async def _do_pvp(self, cli: RemangaClient):
        code, resp = await cli.pvp()
        if code == 200:
            winner = resp.get("winner") or (resp.get("battle") or {}).get("winner") or ""
            await self.notify(f"🏆 PvP: {winner or 'бой завершён'}")
        elif code == 0:
            await self.notify("⚠️ PvP: сетевая ошибка")

    async def _do_raid(self, cli: RemangaClient, loc: int):
        code, resp = await cli.raid(loc)
        if code == 200:
            await self.notify(f"⚔️ Рейд локации {loc}: успешно")
        else:
            err = resp.get("detail") or resp.get("error") or resp
            await self.notify(f"⚠️ Рейд {loc}: HTTP {code} — {err}")

    async def _do_cata(self, cli: RemangaClient):
        state = await cli.cata_state() or {}

        pending = state.get("mini_game") or (state.get("current_run") or {}).get("mini_game")
        pending_id = (pending or {}).get("attempt_id") or state.get("attempt_id")
        if pending_id:
            await cli.cata_resolve(pending_id)
            await self.notify("🏺 Дорешана незакрытая мини-игра")

        cleared = state.get("cleared_stars") or {}
        lvl = stars = None
        for i in range(1, 26):
            s = int(cleared.get(str(i), 0) or 0)
            if s < 5:
                lvl, stars = i, s + 1
                break
        if not lvl:
            return

        code, resp = await cli.cata_enter(lvl, stars)
        if code != 200:
            err = resp.get("detail") or resp.get("error") or resp
            await self.notify(f"🏺 Катакомбы: ошибка входа {lvl}★{stars} — {err}")
            return

        kind = resp.get("kind")
        if kind == "mini_game_required":
            aid = (resp.get("mini_game") or {}).get("attempt_id") or resp.get("attempt_id")
            if aid:
                await cli.cata_resolve(aid)
                await self.notify(f"🏺 Катакомбы: ярус {lvl} ({stars}★) + мини-игра")
            else:
                await self.notify(f"🏺 Катакомбы: {lvl} ({stars}★) — мини-игра без ID")
        elif kind == "run_finished":
            await self.notify(f"🏺 Катакомбы: ярус {lvl} ({stars}★) пройден")
        else:
            await self.notify(f"🏺 Катакомбы: ярус {lvl} ({stars}★) — {kind or 'ok'}")


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
# 5. TELEGRAM-ХЕНДЛЕРЫ
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
    kb.button(text=f"{'✅' if u['raid_enabled'] else '❌'} Рейд",      callback_data="tgl:raid_enabled")
    kb.button(text=f"{'✅' if u['pvp_enabled']  else '❌'} PvP",       callback_data="tgl:pvp_enabled")
    kb.button(text=f"{'✅' if u['cata_enabled'] else '❌'} Катакомбы", callback_data="tgl:cata_enabled")
    kb.button(text=f"📍 Локация: {u['raid_loc']}",                    callback_data="raid:loc")
    kb.button(text="⬅️ Назад", callback_data="ctl:menu")
    kb.adjust(1, 1, 1, 1, 1)
    return kb.as_markup()


def raid_loc_kb(current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for loc in range(1, 11):
        kb.button(
            text=("• " if loc == current else "") + f"Локация {loc}",
            callback_data=f"raid:set:{loc}",
        )
    kb.button(text="⬅️ Назад", callback_data="ctl:settings")
    kb.adjust(3, 3, 3, 1, 1)
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
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка соединения: {e}")
        return

    await save_user(m.chat.id, login, password)
    await save_token(m.chat.id, token)

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
        p = await cli.profile()
    finally:
        await cli.close()
    if not p:
        await cq.answer("Не удалось получить профиль", show_alert=True)
        return
    text = (
        "👤 <b>Профиль</b>\n"
        f"⚡ Энергия: {p.get('energy_current', '?')}/{p.get('energy_max', '?')}\n"
        f"🏆 PvP: {p.get('pvp_wins', 0)}W / {p.get('pvp_losses', 0)}L\n"
        f"⭐ Звёзды: {p.get('awakening_energy', '?')}\n"
        f"🎲 Рероллы: {p.get('full_reroll_energy', '?')}\n"
        f"💠 Пыль: {p.get('reroll_dust', '?')}"
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
    await cq.message.edit_text("📍 Выберите локацию рейда:", reply_markup=raid_loc_kb(u["raid_loc"]))
    await cq.answer()


@router.callback_query(F.data.startswith("raid:set:"))
async def cb_raid_set(cq: CallbackQuery):
    loc = int(cq.data.split(":")[2])
    await update_flag(cq.from_user.id, "raid_loc", loc)
    u = await get_user(cq.from_user.id)
    await cq.message.edit_text("⚙️ Настройки:", reply_markup=settings_kb(u))
    await cq.answer(f"Локация {loc}")


@router.callback_query(F.data == "ctl:logout")
async def cb_logout(cq: CallbackQuery):
    await stop_battler(cq.from_user.id)
    await delete_user(cq.from_user.id)
    await cq.message.edit_text("🚪 Аккаунт удалён.")
    await cq.answer()


# ============================================================
# 6. HEALTH-СЕРВЕР
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
    log.info("Health endpoint listening on :%d", PORT)
    return runner


# ============================================================
# 7. ЗАПУСК
# ============================================================

def make_bot() -> Bot:
    """
    Создаёт Bot с принудительным IPv4 и увеличенными таймаутами.
    Исправляет TelegramNetworkError: Request timeout error
    на серверах с кривым IPv6-маршрутом к api.telegram.org.
    """
    connector = TCPConnector(family=socket.AF_INET)  # только IPv4
    session = AiohttpSession()
    session._connector = connector
    # Увеличиваем таймауты (в секундах)
    session._connector_limit = 100
    session._connector_limit_per_host = 20

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
        timeout=120,  # таймаут на запросы к Telegram API
    )
    return bot


async def main():
    await init_db()

    health_runner = await start_health_server()

    bot = make_bot()
    dp = Dispatcher()
    dp.include_router(router)

    log.info("Bot polling started (forced IPv4)")
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
