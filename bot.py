# ============================================================
# Remanga AutoBattle Telegram Bot — multi-account edition v2
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

# ---------- Логирование ----------
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("remanga-bot")

# ---------- Конфиг ----------
load_dotenv(override=False)
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "").strip()
FERNET_KEY   = os.environ.get("FERNET_KEY", "").strip()
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PORT         = int(os.environ.get("PORT", "8080"))

if not BOT_TOKEN or not FERNET_KEY or not DATABASE_URL:
    raise RuntimeError("Не заданы BOT_TOKEN / FERNET_KEY / DATABASE_URL")

fernet = Fernet(FERNET_KEY.encode())

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]
for junk in ("?sslmode=require", "&sslmode=require",
             "?ssl=true", "&ssl=true",
             "?channel_binding=require", "&channel_binding=require"):
    DATABASE_URL = DATABASE_URL.replace(junk, "")

MAX_ACCOUNTS_PER_USER = 10
API_DOMAIN = "https://api.remanga.org"

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.91",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def ua_for(account_id: int) -> str:
    return USER_AGENTS[account_id % len(USER_AGENTS)]

# ---------- Интервалы ----------
REMANGA_CONCURRENCY       = 40
REMANGA_CONCURRENCY_SLOW  = 15
PVP_INTERVAL              = 32
PVP_JITTER                = 3
PVP_INTERVAL_THROTTLED    = 60
RATE_LIMIT_WINDOW         = 60
RATE_LIMIT_THRESHOLD      = 5
RAID_INTERVAL             = 300
STATUS_INTERVAL           = 3600
BATTLE_LOOP_INTERVAL      = 600
BATTLE_LOOP_INTERVAL_IDLE = 900
SERVER_DOWN_RETRY         = 300
START_JITTER              = 30
MINI_GAME_MAX_ATTEMPTS    = 3

RAID_ENERGY_COST = {1: 4, 2: 5, 3: 6, 4: 7, 5: 8, 6: 9, 7: 10, 8: 11, 9: 12, 10: 13}

SHOP_AWAKENING_ENERGY_ID = 6333
SHOP_COST_EVENT_POINTS   = 35
FORBIDDEN_SHOP_IDS       = {6332}
MAX_STARS_AUTO_BUY       = 10

# ---------- МСК ----------
MSK = timezone(timedelta(hours=3))
CATA_RESURRECT_LIMIT = 3

def msk_now() -> datetime: return datetime.now(MSK)

def msk_in_schedule(start: int, end: int) -> bool:
    """
    Расписание по МСК.
    0/0 → 24/7.
    start < end → окно [start, end).
    start > end → окно через полночь (например, 23→3).
    """
    if start == 0 and end == 0:
        return True
    h = msk_now().hour
    if start < end:
        return start <= h < end
    return h >= start or h < end

# ---------- Динамический семафор ----------
class DynamicSemaphore:
    def __init__(self, limit: int):
        self._limit = limit
        self._current = 0
        self._cond = asyncio.Condition()
    @property
    def limit(self): return self._limit
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
    async def __aexit__(self, *a):
        async with self._cond:
            self._current -= 1
            self._cond.notify()

_remanga_sem = DynamicSemaphore(REMANGA_CONCURRENCY)

# ---------- 429 ----------
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
        log.warning("Rate limit: %d 429 in %ds. Throttling 5 min.", len(recent), RATE_LIMIT_WINDOW)
    elif _is_throttled() and now >= _rate_limit_throttled_until:
        _rate_limit_throttled_until = 0
        await _remanga_sem.set_limit(REMANGA_CONCURRENCY)
        _rate_limit_times.clear()
        log.info("Rate limit recovered.")

async def _rate_limit_watcher():
    while True:
        try:
            await _maybe_throttle_or_unthrottle()
        except Exception:
            log.exception("rate limit watcher")
        await asyncio.sleep(15)

# ============================================================
# БАЗА ДАННЫХ
# ============================================================
_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=5, ssl=False,
            command_timeout=30, statement_cache_size=0,
        )
    return _pool

async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id         BIGINT PRIMARY KEY,
    notify_mode     TEXT    DEFAULT 'important',
    schedule_start  INTEGER DEFAULT 0,
    schedule_end    INTEGER DEFAULT 0,
    created_at      BIGINT  DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);
CREATE TABLE IF NOT EXISTS accounts (
    id            BIGSERIAL PRIMARY KEY,
    chat_id       BIGINT NOT NULL REFERENCES users(chat_id) ON DELETE CASCADE,
    label         TEXT NOT NULL DEFAULT '',
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
    auto_start    INTEGER DEFAULT 0,
    created_at    BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
);
CREATE INDEX IF NOT EXISTS idx_accounts_chat ON accounts(chat_id);
"""

MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_mode TEXT DEFAULT 'important'",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS schedule_start INTEGER DEFAULT 0",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS schedule_end INTEGER DEFAULT 0",
    "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS auto_start INTEGER DEFAULT 0",
]

async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(INIT_SQL)
        for sql in MIGRATIONS:
            try: await conn.execute(sql)
            except Exception: pass
    safe = DATABASE_URL.split("@")[-1].split("/")[0] if "@" in DATABASE_URL else "?"
    log.info("DB ready (Postgres @ %s)", safe)

async def ensure_user(chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (chat_id) VALUES ($1) ON CONFLICT DO NOTHING",
            chat_id)

async def get_user_prefs(chat_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT notify_mode, schedule_start, schedule_end FROM users WHERE chat_id=$1",
            chat_id)
    if not row:
        return {"notify_mode": "important", "schedule_start": 0, "schedule_end": 0}
    return {
        "notify_mode":    (row["notify_mode"] or "important"),
        "schedule_start": int(row["schedule_start"] or 0),
        "schedule_end":   int(row["schedule_end"] or 0),
    }

async def update_user_pref(chat_id: int, field: str, value):
    assert field in {"notify_mode", "schedule_start", "schedule_end"}
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE users SET {field}=$1 WHERE chat_id=$2", value, chat_id)

async def count_accounts(chat_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM accounts WHERE chat_id=$1", chat_id)

async def add_account(chat_id: int, login: str, password: str, label: str) -> int | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO accounts (chat_id, label, login_enc, password_enc)
               VALUES ($1, $2, $3, $4) RETURNING id""",
            chat_id, label,
            fernet.encrypt(login.encode()),
            fernet.encrypt(password.encode()))
    return row["id"] if row else None

async def save_account_token(account_id: int, token: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE accounts SET token_enc=$1 WHERE id=$2",
            fernet.encrypt(token.encode()), account_id)

def _acc_from_row(row) -> dict | None:
    if not row: return None
    d = dict(row)
    try:
        login = fernet.decrypt(d["login_enc"]).decode()
        password = fernet.decrypt(d["password_enc"]).decode()
        token = fernet.decrypt(d["token_enc"]).decode() if d.get("token_enc") else None
    except Exception as e:
        log.warning("Bad account id=%s: %s", d.get("id"), e)
        return None
    return {
        "id":            d["id"],
        "chat_id":       d["chat_id"],
        "label":         d.get("label") or f"Acc#{d['id']}",
        "login":         login,
        "password":      password,
        "token":         token,
        "raid_enabled":  bool(d.get("raid_enabled", 1)),
        "pvp_enabled":   bool(d.get("pvp_enabled", 1)),
        "cata_enabled":  bool(d.get("cata_enabled", 1)),
        "raid_loc":      d.get("raid_loc", 10),
        "cata_fixed":    bool(d.get("cata_fixed", 1)),
        "cata_level":    d.get("cata_level", 17),
        "cata_stars":    d.get("cata_stars", 1),
        "auto_start":    bool(d.get("auto_start", 0)),
    }

async def get_account(account_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM accounts WHERE id=$1", account_id)
    return _acc_from_row(row)

async def list_accounts(chat_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM accounts WHERE chat_id=$1 ORDER BY id", chat_id)
    return [a for a in (_acc_from_row(r) for r in rows) if a]

ALLOWED_FIELDS = {
    "raid_enabled", "pvp_enabled", "cata_enabled", "raid_loc",
    "cata_fixed", "cata_level", "cata_stars", "label", "auto_start",
}

async def update_account_flag(account_id: int, field: str, value):
    assert field in ALLOWED_FIELDS
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE accounts SET {field}=$1 WHERE id=$2", value, account_id)

async def delete_account(account_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM accounts WHERE id=$1", account_id)

async def all_accounts_for_startup() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM accounts")
    return [a for a in (_acc_from_row(r) for r in rows) if a]

async def autostart_accounts() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM accounts WHERE auto_start=1 AND token_enc IS NOT NULL")
    return [a for a in (_acc_from_row(r) for r in rows) if a and a.get("token")]

# ============================================================
# КЛИЕНТ REMANGA
# ============================================================
class RemangaAuthError(Exception): pass

def _extract_token(data, depth: int = 0):
    if depth > 4 or not isinstance(data, (dict, list)): return None
    if isinstance(data, list):
        for it in data:
            t = _extract_token(it, depth + 1)
            if t: return t
        return None
    for k in ("token","access_token","accessToken","access","jwt",
              "auth_token","authToken","key","id_token","bearer"):
        v = data.get(k)
        if isinstance(v, str) and len(v) > 20: return v
    for w in ("content","data","result","user","profile","auth"):
        wv = data.get(w)
        if isinstance(wv, (dict, list)):
            t = _extract_token(wv, depth + 1)
            if t: return t
    for v in data.values():
        if isinstance(v, (dict, list)):
            t = _extract_token(v, depth + 1)
            if t: return t
    return None

class RemangaClient:
    def __init__(self, token: str | None = None, account_id: int = 0):
        self.token = self._norm(token) if token else None
        self._c = httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": ua_for(account_id), "Accept": "application/json"})
    @staticmethod
    def _norm(t: str) -> str:
        t = t.strip()
        return t if t.startswith("Bearer ") else f"Bearer {t}"
    async def close(self): await self._c.aclose()
    def _h(self): return {"Authorization": self.token} if self.token else {}

    @staticmethod
    async def login(login: str, password: str, account_id: int = 0) -> str:
        ua = ua_for(account_id)
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                f"{API_DOMAIN}/api/users/login/",
                json={"user": login, "password": password},
                headers={"Content-Type": "application/json",
                         "User-Agent": ua, "Accept": "application/json"})
            log.info("LOGIN status=%s len=%d", r.status_code, len(r.text or ""))
            if r.status_code != 200:
                if r.status_code in (400, 401, 403):
                    raise RemangaAuthError("Неверный логин/пароль")
                raise RemangaAuthError(f"Ошибка входа: HTTP {r.status_code}")
            try: data = r.json()
            except Exception: data = {}
            token = _extract_token(data)
            if not token:
                for cn in ("token","access_token","accessToken","authorization","jwt","auth"):
                    try: v = r.cookies.get(cn)
                    except Exception: v = None
                    if v and len(v) > 20: token = v; break
            if not token:
                ah = r.headers.get("authorization") or r.headers.get("Authorization")
                if ah and ah.lower().startswith("bearer "): token = ah[7:]
            if not token: raise RemangaAuthError("Токен не найден.")
            return token

    async def _get(self, path: str, retries: int = 2):
        async with _remanga_sem:
            for attempt in range(retries + 1):
                try:
                    r = await self._c.get(f"{API_DOMAIN}{path}", headers=self._h())
                    if r.status_code == 401: raise RemangaAuthError("Токен истёк")
                    if r.status_code == 429:
                        _record_429(); await asyncio.sleep(30); continue
                    if 500 <= r.status_code < 600:
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt); continue
                        return {"_server_down": True}
                    return r.json() if r.status_code == 200 else None
                except (httpx.HTTPError, httpx.TimeoutException):
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt); continue
                    return {"_server_down": True}
            return {"_server_down": True}

    async def _post(self, path: str, body=None, retries: int = 2):
        async with _remanga_sem:
            for attempt in range(retries + 1):
                try:
                    r = await self._c.post(f"{API_DOMAIN}{path}",
                                           json=body or {}, headers=self._h())
                    if r.status_code == 401: raise RemangaAuthError("Токен истёк")
                    if r.status_code == 429:
                        _record_429(); await asyncio.sleep(30); continue
                    if 500 <= r.status_code < 600:
                        if attempt < retries:
                            await asyncio.sleep(2 ** attempt); continue
                        return 0, {"_server_down": True}
                    try: data = r.json()
                    except Exception: data = {"text": r.text[:200]}
                    return r.status_code, data
                except (httpx.HTTPError, httpx.TimeoutException):
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt); continue
                    return 0, {"_server_down": True}
            return 0, {"_server_down": True}

    async def profile(self):          return await self._get("/api/v2/events/card-battle/profile/")
    async def eventpoint_balance(self): return await self._get("/api/v2/events/eventpoint-balance/")
    async def raid(self, loc: int):   return await self._post(f"/api/v2/events/card-battle/locations/{loc}/raid/")
    async def pvp(self):              return await self._post("/api/v2/events/card-battle/pvp/match/")
    async def cata_state(self):       return await self._get("/api/v2/events/card-battle/catacombs/state/")
    async def cata_levels(self):      return await self._get("/api/v2/events/card-battle/catacombs/levels/")
    async def cata_enter(self, lvl, stars):
        return await self._post(f"/api/v2/events/card-battle/catacombs/levels/{lvl}/enter/", {"stars": stars})
    async def cata_resolve(self, aid):
        return await self._post(f"/api/v2/events/card-battle/catacombs/mini-game/{aid}/resolve/",
                                {"outcome": "won", "proof": {}})
    async def buy_shop_item(self, item_id: int, amount: int = 1):
        if item_id in FORBIDDEN_SHOP_IDS: return 403, {"error": "forbidden"}
        return await self._post(f"/api/v2/shop/buy/{item_id}/", {"amount": amount})

def raid_energy(loc: int) -> int:
    return RAID_ENERGY_COST.get(loc, 13)

# ============================================================
# АВТОБОЙ
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

# Глобальный кэш последних статусов — для сводного отчёта
_last_status: dict[int, dict] = {}

class AutoBattler:
    def __init__(self, account_id: int, chat_id: int, label: str,
                 token: str, notify, flags: Flags, user_prefs: dict):
        self.account_id = account_id
        self.chat_id = chat_id
        self.label = label
        self.token = token
        self.notify = notify
        self.flags = flags
        self._user_prefs = user_prefs or {
            "notify_mode": "important", "schedule_start": 0, "schedule_end": 0,
        }
        self.task: asyncio.Task | None = None
        self.pvp_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._mini_attempts: dict[int, int] = {}
        self._pvp_interval = PVP_INTERVAL + (account_id % 7)
        self._raid_interval = RAID_INTERVAL + (account_id % 60)

    def set_user_prefs(self, prefs: dict):
        self._user_prefs = prefs

    def _prefix(self) -> str:
        return f"[{self.label}]"

    async def _emit(self, text: str, important: bool = False):
        log.info("[acc=%s chat=%s] %s", self.account_id, self.chat_id, text)
        mode = self._user_prefs.get("notify_mode", "important")
        if mode == "off":
            return
        if mode == "important" and not important:
            return
        with suppress(Exception):
            await self.notify(f"{self._prefix()} {text}")

    async def start(self):
        if self.task and not self.task.done(): return
        self._stop.clear()
        jitter = random.uniform(0, START_JITTER)
        async def _delayed():
            await asyncio.sleep(jitter)
            await self._run_safe()
        self.task = asyncio.create_task(_delayed())
        if self.flags.pvp:
            self.pvp_task = asyncio.create_task(self._pvp_loop())

    async def stop(self):
        self._stop.set()
        for t in (self.task, self.pvp_task):
            if t:
                t.cancel()
                with suppress(asyncio.CancelledError): await t
        self.task = None
        self.pvp_task = None
        self._mini_attempts.clear()
        _last_status.pop(self.account_id, None)

    async def _run_safe(self):
        while not self._stop.is_set():
            try:
                await self._run(); return
            except asyncio.CancelledError: raise
            except Exception as e:
                log.exception("autobattle crash acc=%s", self.account_id)
                with suppress(Exception):
                    await self.notify(f"{self._prefix()} ⚠️ Перезапуск 30 сек: {e}")
                await asyncio.sleep(30)

    async def _pvp_loop(self):
        cli = RemangaClient(self.token, account_id=self.account_id)
        cooldown = 0
        try:
            while not self._stop.is_set():
                try:
                    if not self.flags.pvp:
                        await asyncio.sleep(5); continue
                    code, resp = await cli.pvp()
                    if code == 200:
                        cooldown = int(resp.get("pvp_cooldown_seconds", 0) or 0)
                        winner = resp.get("winner") or (resp.get("battle") or {}).get("winner") or ""
                        await self._emit(f"🏆 PvP: {winner or 'ok'}", important=False)
                    else:
                        cooldown = 30
                    base = PVP_INTERVAL_THROTTLED if _is_throttled() else self._pvp_interval
                    await asyncio.sleep(max(base, cooldown) + random.uniform(0, PVP_JITTER))
                except RemangaAuthError:
                    await asyncio.sleep(60)
                except asyncio.CancelledError: raise
                except Exception:
                    log.exception("pvp loop acc=%s", self.account_id)
                    await asyncio.sleep(60)
        finally:
            await cli.close()

    async def _resurrect_done(self, cli) -> bool:
        state = await cli.cata_state() or {}
        for s in state.get("scrolls") or []:
            if isinstance(s, dict) and s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0) or CATA_RESURRECT_LIMIT
                return used >= limit
        return False

    @staticmethod
    def _resurrect_line(state: dict) -> str:
        for s in (state or {}).get("scrolls") or []:
            if isinstance(s, dict) and s.get("kind") == "resurrect":
                used = int(s.get("daily_used", 0) or 0)
                limit = int(s.get("daily_limit", 0) or 0)
                qty = int(s.get("quantity", 0) or 0)
                mark = "✅" if used >= limit else "⏳"
                return f"{mark} Воскрешение: {used}/{limit} (в наличии {qty})"
        return "⏳ Воскрешение: нет данных"

    async def _ensure_stars(self, cli, need_stars: int) -> bool:
        profile = await cli.profile()
        if isinstance(profile, dict) and profile.get("_server_down"): return False
        profile = profile or {}
        cur = int(profile.get("awakening_energy", 0) or 0)
        if cur >= need_stars: return True
        if cur >= MAX_STARS_AUTO_BUY:
            await self._emit(f"⛔ Звёзд {cur} ≥ {MAX_STARS_AUTO_BUY} — покупка отключена", important=False)
            return False
        buy_count = min(need_stars - cur, MAX_STARS_AUTO_BUY - cur)
        if buy_count <= 0: return False
        ep = await cli.eventpoint_balance() or {}
        if isinstance(ep, dict) and ep.get("_server_down"): return False
        pts = int(ep.get("balance", 0) or 0)
        need_pts = buy_count * SHOP_COST_EVENT_POINTS
        if pts < need_pts:
            await self._emit(f"⭐ Звёзд {cur}/{need_stars}, points {pts}/{need_pts}. Рейдим.", important=False)
            return False
        bought = 0
        for _ in range(buy_count):
            pn = await cli.profile() or {}
            if isinstance(pn, dict) and pn.get("_server_down"): break
            if int(pn.get("awakening_energy", 0) or 0) >= MAX_STARS_AUTO_BUY: break
            code, resp = await cli.buy_shop_item(SHOP_AWAKENING_ENERGY_ID, 1)
            if code != 200:
                await self._emit(f"⚠️ Покупка: HTTP {code}", important=True); break
            bought += 1
            await asyncio.sleep(1.0)
        if bought > 0:
            await self._emit(f"⭐ Куплено {bought}★ за {bought * SHOP_COST_EVENT_POINTS} pts", important=True)
        pa = await cli.profile() or {}
        return int(pa.get("awakening_energy", 0) or 0) >= need_stars

    async def _run(self):
        cli = RemangaClient(self.token, account_id=self.account_id)
        last_raid = 0
        last_status = 0

        s_start = self._user_prefs.get("schedule_start", 0)
        s_end = self._user_prefs.get("schedule_end", 0)
        if s_start == 0 and s_end == 0:
            sched_desc = "24/7"
        else:
            sched_desc = f"{s_start:02d}:00–{s_end:02d}:00 МСК"

        cata_desc = (f"{self.flags.cata_level}/{self.flags.cata_stars}★"
                     if self.flags.cata_fixed else "авто")
        await self._emit(
            f"🟢 Запущен\n"
            f"⚔️ Рейд: {'вкл' if self.flags.raid else 'выкл'} (лок. {self.flags.raid_loc})\n"
            f"🏆 PvP: {'вкл' if self.flags.pvp else 'выкл'}\n"
            f"🏺 Катакомбы: {'вкл' if self.flags.cata else 'выкл'} — {cata_desc}\n"
            f"🕒 Расписание: {sched_desc}",
            important=True)

        cata_levels_cache: list = []
        async def get_levels():
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
                        await self._emit("🌐 Сервер недоступен, жду 5 мин", important=False)
                        await asyncio.sleep(SERVER_DOWN_RETRY); continue
                    profile = profile or {}
                    energy = int(profile.get("energy_current", 0) or 0)
                    stars = int(profile.get("awakening_energy", 0) or 0)

                    reserve = need_stars_cata = 0
                    if self.flags.cata and self.flags.cata_fixed:
                        levels = await get_levels()
                        if isinstance(levels, dict) and levels.get("_server_down"):
                            await asyncio.sleep(SERVER_DOWN_RETRY); continue
                        cfg = next((x for x in (levels or []) if x.get("level") == self.flags.cata_level), None)
                        if cfg:
                            reserve = int(cfg.get("energy_cost", {}).get(str(self.flags.cata_stars), 0) or 0)
                            need_stars_cata = int(cfg.get("awakening_energy_cost", {}).get(str(self.flags.cata_stars), 0) or 0)

                    # --- расписание (обновляем из self._user_prefs) ---
                    s_start = self._user_prefs.get("schedule_start", 0)
                    s_end = self._user_prefs.get("schedule_end", 0)
                    in_win = msk_in_schedule(s_start, s_end)

                    res_done = True
                    if self.flags.cata:
                        res_done = await self._resurrect_done(cli)
                    cata_open = self.flags.cata and in_win and not res_done
                    next_sleep = BATTLE_LOOP_INTERVAL if cata_open else BATTLE_LOOP_INTERVAL_IDLE

                    # Катакомбы
                    stars_ok = True
                    if cata_open and stars < need_stars_cata:
                        stars_ok = await self._ensure_stars(cli, need_stars_cata)
                        p2 = await cli.profile() or {}
                        if not (isinstance(p2, dict) and p2.get("_server_down")):
                            stars = int(p2.get("awakening_energy", 0) or 0)

                    if cata_open and stars_ok and energy >= reserve and stars >= need_stars_cata:
                        await self._do_cata(cli)
                        await asyncio.sleep(BATTLE_LOOP_INTERVAL); continue

                    # Рейд
                    cata_ready = cata_open and energy >= reserve and stars >= need_stars_cata
                    if self.flags.raid and not cata_ready and now - last_raid >= self._raid_interval:
                        last_raid = now
                        need_raid = raid_energy(self.flags.raid_loc)
                        if energy >= need_raid:
                            code, resp = await cli.raid(self.flags.raid_loc)
                            if isinstance(resp, dict) and resp.get("_server_down"):
                                last_raid = now - self._raid_interval + 60
                                await asyncio.sleep(SERVER_DOWN_RETRY); continue
                            if code == 200:
                                rewards = resp.get("rewards") or []
                                rtxt = ", ".join(f"{r.get('kind')} x{r.get('amount')}"
                                                 for r in rewards if isinstance(r, dict)) or "ok"
                                await self._emit(f"⚔️ Рейд {self.flags.raid_loc}: {rtxt}", important=False)

                    # Сбор статуса для сводного отчёта (каждые 5 минут достаточно)
                    if now - last_status >= 300:
                        last_status = now
                        dust = int(profile.get("reroll_dust", 0) or 0)
                        rerolls = int(profile.get("full_reroll_energy", 0) or 0)
                        ep = await cli.eventpoint_balance() or {}
                        if isinstance(ep, dict) and ep.get("_server_down"): ep = {}
                        pts = int(ep.get("balance", 0) or 0)
                        state = await cli.cata_state() or {}
                        if isinstance(state, dict) and state.get("_server_down"): state = {}
                        rline = self._resurrect_line(state)
                        _last_status[self.account_id] = {
                            "label": self.label,
                            "chat_id": self.chat_id,
                            "energy": energy, "stars": stars,
                            "dust": dust, "rerolls": rerolls, "points": pts,
                            "resurrect": rline, "res_done": res_done,
                            "time": now,
                        }

                except RemangaAuthError:
                    await self._emit("🔐 Токен истёк. Удалите и добавьте заново.", important=True)
                    break
                except asyncio.CancelledError: raise
                except Exception as e:
                    log.exception("acc loop error acc=%s", self.account_id)
                    await self._emit(f"❌ Ошибка: {e}", important=True)
                await asyncio.sleep(next_sleep)
        finally:
            await cli.close()

    async def _do_cata(self, cli):
        state = await cli.cata_state()
        if isinstance(state, dict) and state.get("_server_down"): return
        state = state or {}
        profile = await cli.profile()
        if isinstance(profile, dict) and profile.get("_server_down"): return
        profile = profile or {}

        pending = state.get("mini_game") or (state.get("current_run") or {}).get("mini_game")
        pid = (pending or {}).get("attempt_id") or state.get("attempt_id")
        if pid:
            att = self._mini_attempts.get(pid, 0)
            if att < MINI_GAME_MAX_ATTEMPTS:
                code, resp = await cli.cata_resolve(pid)
                if code == 200 and not (isinstance(resp, dict) and resp.get("_server_down")):
                    self._mini_attempts.pop(pid, None)
                    await self._emit("🏺 Дорешана мини-игра", important=False)
                else:
                    self._mini_attempts[pid] = att + 1
            else:
                await self._emit(f"🚫 Мини-игра {pid} пропущена", important=True)
                self._mini_attempts.pop(pid, None); return

        if self.flags.cata_fixed:
            lvl, stars = self.flags.cata_level, self.flags.cata_stars
        else:
            cleared = state.get("cleared_stars") or {}
            lvl = stars = None
            for i in range(1, 26):
                if int(cleared.get(str(i), 0) or 0) < 5:
                    lvl = i
                    stars = int(cleared.get(str(i), 0) or 0) + 1
                    break
        if not lvl: return

        levels = await cli.cata_levels()
        if isinstance(levels, dict) and levels.get("_server_down"): return
        cfg = next((x for x in (levels or []) if x.get("level") == lvl), None)
        if not cfg: return

        ne = int(cfg.get("energy_cost", {}).get(str(stars), 0) or 0)
        ns = int(cfg.get("awakening_energy_cost", {}).get(str(stars), 0) or 0)
        nr = int(cfg.get("full_reroll_energy_cost", {}).get(str(stars), 0) or 0)

        ce = int(profile.get("energy_current", 0) or 0)
        cs = int(profile.get("awakening_energy", 0) or 0)
        cr = int(profile.get("full_reroll_energy", 0) or 0)

        if ce < ne or cs < ns or cr < nr:
            await self._emit(f"⏸ {lvl}/{stars}★: не хватает", important=False)
            return

        code, resp = await cli.cata_enter(lvl, stars)
        if isinstance(resp, dict) and resp.get("_server_down"): return
        if code != 200:
            await self._emit(f"🏺 Катакомбы {lvl}/{stars}★: ошибка {code}", important=True); return

        kind = resp.get("kind")
        if kind == "mini_game_required":
            aid = (resp.get("mini_game") or {}).get("attempt_id") or resp.get("attempt_id")
            if aid:
                code2, resp2 = await cli.cata_resolve(aid)
                reward = "мини-игра пройдена" if (code2 == 200 and not (isinstance(resp2, dict) and resp2.get("_server_down"))) else "мини-игра не решена"
            else:
                reward = "мини-игра без ID"
        elif kind == "run_finished":
            rewards = resp.get("rewards") or []
            reward = ", ".join(f"{r.get('kind')} x{r.get('amount')}"
                               for r in rewards if isinstance(r, dict)) or "ok"
        else:
            reward = str(kind or "ok")

        new_state = await cli.cata_state() or {}
        if isinstance(new_state, dict) and new_state.get("_server_down"): new_state = {}
        rline = self._resurrect_line(new_state)
        await self._emit(f"🏺 Катакомбы {lvl}/{stars}★: {reward}\n{rline}", important=True)

# ============================================================
# РЕЕСТР
# ============================================================
_battlers: dict[int, AutoBattler] = {}
_registry_lock = asyncio.Lock()

def is_running(account_id: int) -> bool:
    b = _battlers.get(account_id)
    return bool(b and b.task and not b.task.done())

async def register_battler(account: dict, notify, user_prefs: dict) -> AutoBattler:
    async with _registry_lock:
        old = _battlers.pop(account["id"], None)
        if old: await old.stop()
        flags = Flags(
            raid=account["raid_enabled"], pvp=account["pvp_enabled"],
            cata=account["cata_enabled"], raid_loc=account["raid_loc"],
            cata_fixed=account["cata_fixed"], cata_level=account["cata_level"],
            cata_stars=account["cata_stars"],
        )
        b = AutoBattler(account["id"], account["chat_id"], account["label"],
                        account["token"], notify, flags, user_prefs)
        _battlers[account["id"]] = b
        await b.start()
        return b

async def stop_battler(account_id: int):
    async with _registry_lock:
        b = _battlers.pop(account_id, None)
    if b: await b.stop()

async def refresh_user_prefs(chat_id: int):
    prefs = await get_user_prefs(chat_id)
    for acc_id, b in list(_battlers.items()):
        if b.chat_id == chat_id:
            b.set_user_prefs(prefs)

def accounts_of_chat(chat_id: int) -> list[int]:
    return [aid for aid, b in _battlers.items() if b.chat_id == chat_id]

# ============================================================
# ФОНОВЫЕ ВОРКЕРЫ
# ============================================================
async def _hourly_report_loop(bot: Bot):
    """Раз в час: одно сообщение на юзера со статусами всех его активных аккаунтов."""
    await asyncio.sleep(STATUS_INTERVAL)
    while True:
        try:
            by_chat: dict[int, list[dict]] = {}
            now = time.time()
            for acc_id, b in list(_battlers.items()):
                st = _last_status.get(acc_id)
                if st and now - st["time"] < 7200:
                    by_chat.setdefault(b.chat_id, []).append(st)
            for chat_id, accs in by_chat.items():
                prefs = await get_user_prefs(chat_id)
                if prefs["notify_mode"] == "off":
                    continue
                lines = [f"📊 Отчёт ({len(accs)} акк.):"]
                for a in accs:
                    rline = a.get("resurrect", "")
                    lines.append(
                        f"\n• <b>{a['label']}</b>\n"
                        f"  ⚡ {a['energy']}  ⭐ {a['stars']}  💠 {a['dust']}  🎯 {a['points']}\n"
                        f"  {rline}"
                    )
                with suppress(Exception):
                    await bot.send_message(chat_id, "\n".join(lines))
        except Exception:
            log.exception("hourly report error")
        await asyncio.sleep(STATUS_INTERVAL)

async def _autostart_accounts(bot: Bot):
    """Запуск аккаунтов с auto_start=1 после рестарта."""
    accounts = await autostart_accounts()
    if not accounts:
        return
    log.info("Autostart: %d accounts", len(accounts))
    for a in accounts:
        try:
            prefs = await get_user_prefs(a["chat_id"])
            notify = _notify_for(bot, a["chat_id"])
            await register_battler(a, notify, prefs)
        except Exception:
            log.exception("autostart failed acc=%s", a["id"])

# ============================================================
# ХЕНДЛЕРЫ
# ============================================================
router = Router()

def main_menu_kb(accounts: list[dict], prefs: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for a in accounts:
        running = is_running(a["id"])
        emoji = "🟢" if running else "🔴"
        auto = "🔁" if a.get("auto_start") else ""
        label = (a["label"][:20] or f"Acc#{a['id']}")
        kb.button(text=f"{emoji}{auto} {label}", callback_data=f"acc:view:{a['id']}")
    kb.button(text="➕ Добавить", callback_data="acc:add")
    kb.button(text="▶️ Старт все", callback_data="ctl:startall")
    kb.button(text="⏹ Стоп все",  callback_data="ctl:stopall")
    kb.button(text="⚙️ Общие настройки", callback_data="ctl:user_settings")
    kb.adjust(*([1] * len(accounts)), 2, 2, 1)
    return kb.as_markup()

def account_kb(a: dict) -> InlineKeyboardMarkup:
    running = is_running(a["id"])
    auto = bool(a.get("auto_start"))
    kb = InlineKeyboardBuilder()
    if running:
        kb.button(text="⏹ Стоп", callback_data=f"acc:stop:{a['id']}")
    else:
        kb.button(text="▶️ Старт", callback_data=f"acc:start:{a['id']}")
    kb.button(text="⚙️ Настройки", callback_data=f"acc:settings:{a['id']}")
    kb.button(text="ℹ️ Профиль",   callback_data=f"acc:profile:{a['id']}")
    kb.button(text=f"🔁 Автозапуск: {'вкл' if auto else 'выкл'}", callback_data=f"acc:autostart:{a['id']}")
    kb.button(text="🗑 Удалить",   callback_data=f"acc:del:{a['id']}")
    kb.button(text="⬅️ Назад",     callback_data="ctl:menu")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()

def settings_kb(a: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{'✅' if a['raid_enabled'] else '❌'} Рейд",
              callback_data=f"tgl:{a['id']}:raid_enabled")
    kb.button(text=f"{'✅' if a['pvp_enabled'] else '❌'} PvP",
              callback_data=f"tgl:{a['id']}:pvp_enabled")
    kb.button(text=f"{'✅' if a['cata_enabled'] else '❌'} Катакомбы",
              callback_data=f"tgl:{a['id']}:cata_enabled")
    kb.button(text=f"📍 Рейд-локация: {a['raid_loc']}",
              callback_data=f"raidloc:{a['id']}")
    cata_desc = (f"🎯 Катакомбы: {a['cata_level']}/{a['cata_stars']}★"
                 if a["cata_fixed"] else "🎯 Катакомбы: авто")
    kb.button(text=cata_desc, callback_data=f"cata:menu:{a['id']}")
    kb.button(text="⬅️ Назад", callback_data=f"acc:view:{a['id']}")
    kb.adjust(1, 1, 1, 1, 1, 1)
    return kb.as_markup()

def raid_loc_kb(aid: int, current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for loc in range(1, 11):
        kb.button(text=("• " if loc == current else "") + f"Локация {loc}",
                  callback_data=f"raidloc:set:{aid}:{loc}")
    kb.button(text="⬅️ Назад", callback_data=f"acc:settings:{aid}")
    kb.adjust(3, 3, 3, 1)
    return kb.as_markup()

def cata_menu_kb(a: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=f"{'✅' if not a['cata_fixed'] else '▫️'} Авто",
              callback_data=f"cata:auto:{a['id']}")
    kb.button(text=f"{'✅' if a['cata_fixed'] else '▫️'} Фикс.",
              callback_data=f"cata:fixed:{a['id']}")
    kb.button(text=f"📍 Уровень: {a['cata_level']}",
              callback_data=f"cata:lvl:{a['id']}")
    kb.button(text=f"⭐ Сложность: {a['cata_stars']}★",
              callback_data=f"cata:stars:{a['id']}")
    kb.button(text="⬅️ Назад", callback_data=f"acc:settings:{a['id']}")
    kb.adjust(2, 1, 1, 1)
    return kb.as_markup()

def cata_lvl_kb(aid: int, current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for lvl in range(1, 26):
        kb.button(text=("• " if lvl == current else "") + f"{lvl}",
                  callback_data=f"cata:setlvl:{aid}:{lvl}")
    kb.button(text="⬅️ Назад", callback_data=f"cata:menu:{aid}")
    kb.adjust(5, 5, 5, 5, 5, 1)
    return kb.as_markup()

def cata_stars_kb(aid: int, current: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for s in range(1, 6):
        kb.button(text=("• " if s == current else "") + f"{s}★",
                  callback_data=f"cata:setstars:{aid}:{s}")
    kb.button(text="⬅️ Назад", callback_data=f"cata:menu:{aid}")
    kb.adjust(5, 1)
    return kb.as_markup()

def user_settings_kb(prefs: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    mode = prefs["notify_mode"]
    mode_label = {"all": "все", "important": "важные", "off": "выкл"}[mode]
    kb.button(text=f"🔔 Уведомления: {mode_label}", callback_data="uset:notify_menu")
    s, e = prefs["schedule_start"], prefs["schedule_end"]
    sched_label = "24/7" if (s == 0 and e == 0) else f"{s:02d}–{e:02d} МСК"
    kb.button(text=f"🕒 Расписание: {sched_label}", callback_data="uset:sched_menu")
    kb.button(text="⬅️ Назад", callback_data="ctl:menu")
    kb.adjust(1, 1, 1)
    return kb.as_markup()

def notify_menu_kb(current: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for val, name in [("all", "Все"), ("important", "Только важные"), ("off", "Выкл")]:
        mark = "✅ " if val == current else "▫️ "
        kb.button(text=f"{mark}{name}", callback_data=f"uset:notify:{val}")
    kb.button(text="⬅️ Назад", callback_data="ctl:user_settings")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()

def schedule_menu_kb(s: int, e: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    presets = [("24/7", 0, 0), ("03–23", 3, 23), ("05–22", 5, 22),
               ("08–20", 8, 20), ("10–02", 10, 2)]
    for name, ps, pe in presets:
        mark = "✅ " if (ps == s and pe == e) else "▫️ "
        kb.button(text=f"{mark}{name}", callback_data=f"uset:sched:{ps}:{pe}")
    kb.button(text="✍️ Своё (в чат: /schedule 3 23)", callback_data="uset:sched_help")
    kb.button(text="⬅️ Назад", callback_data="ctl:user_settings")
    kb.adjust(3, 2, 1, 1)
    return kb.as_markup()

def _notify_for(bot: Bot, chat_id: int):
    async def notify(text: str):
        with suppress(Exception):
            await bot.send_message(chat_id, text)
    return notify

# ---------- Команды ----------
@router.message(Command("start"))
async def cmd_start(m: Message):
    await ensure_user(m.chat.id)
    accounts = await list_accounts(m.chat.id)
    prefs = await get_user_prefs(m.chat.id)
    if not accounts:
        await m.answer(
            "👋 <b>Remanga AutoBattle Bot</b>\n\n"
            "У вас нет аккаунтов.\n"
            "Добавьте: <code>/add логин пароль [метка]</code>"
        )
        return
    await m.answer(
        f"👋 Аккаунты ({len(accounts)}/{MAX_ACCOUNTS_PER_USER}):",
        reply_markup=main_menu_kb(accounts, prefs))

@router.message(Command("add"))
async def cmd_add(m: Message, command: CommandObject):
    if not command.args:
        await m.answer("Формат: <code>/add логин пароль [метка]</code>")
        return
    parts = command.args.split(maxsplit=2)
    if len(parts) < 2:
        await m.answer("Формат: <code>/add логин пароль [метка]</code>")
        return
    login, password = parts[0], parts[1]
    label = parts[2] if len(parts) > 2 else login[:20]

    await ensure_user(m.chat.id)
    if await count_accounts(m.chat.id) >= MAX_ACCOUNTS_PER_USER:
        await m.answer(f"❌ Лимит: {MAX_ACCOUNTS_PER_USER} аккаунтов.")
        return

    msg = await m.answer(f"🔐 Проверяю «{label}»...")
    cnt = await count_accounts(m.chat.id)
    try:
        token = await RemangaClient.login(login, password, account_id=cnt)
    except RemangaAuthError as e:
        await msg.edit_text(f"❌ {e}")
        return
    except Exception:
        await msg.edit_text("❌ Ошибка соединения.")
        return

    acc_id = await add_account(m.chat.id, login, password, label)
    if not acc_id:
        await msg.edit_text("❌ Ошибка БД.")
        return
    await save_account_token(acc_id, token)

    with suppress(Exception):
        await m.delete()

    accounts = await list_accounts(m.chat.id)
    prefs = await get_user_prefs(m.chat.id)
    await msg.edit_text(
        f"✅ «{label}» добавлен (ID {acc_id}).\nВсего: {len(accounts)}/{MAX_ACCOUNTS_PER_USER}",
        reply_markup=main_menu_kb(accounts, prefs))

@router.message(Command("logout"))
async def cmd_logout(m: Message):
    accounts = await list_accounts(m.chat.id)
    for a in accounts:
        await stop_battler(a["id"])
        await delete_account(a["id"])
    await m.answer(f"🚪 Удалено: {len(accounts)}")

@router.message(Command("list"))
async def cmd_list(m: Message):
    accounts = await list_accounts(m.chat.id)
    if not accounts:
        await m.answer("Нет аккаунтов.")
        return
    prefs = await get_user_prefs(m.chat.id)
    await m.answer(
        f"📋 Аккаунты ({len(accounts)}):",
        reply_markup=main_menu_kb(accounts, prefs))

@router.message(Command("schedule"))
async def cmd_schedule(m: Message, command: CommandObject):
    if not command.args:
        prefs = await get_user_prefs(m.chat.id)
        s, e = prefs["schedule_start"], prefs["schedule_end"]
        await m.answer(
            f"Текущее расписание: {'24/7' if s == 0 and e == 0 else f'{s:02d}–{e:02d} МСК'}\n"
            f"Формат: <code>/schedule start end</code>\n"
            f"Пример: <code>/schedule 3 23</code>\n"
            f"24/7: <code>/schedule 0 0</code>")
        return
    try:
        parts = command.args.split()
        s, e = int(parts[0]), int(parts[1])
        if not (0 <= s <= 23 and 0 <= e <= 24):
            raise ValueError
    except Exception:
        await m.answer("Формат: <code>/schedule start end</code> (часы 0–23)")
        return
    if e == 24: e = 0
    await update_user_pref(m.chat.id, "schedule_start", s)
    await update_user_pref(m.chat.id, "schedule_end", e)
    await refresh_user_prefs(m.chat.id)
    await m.answer(
        f"✅ Расписание: {'24/7' if s == 0 and e == 0 else f'{s:02d}–{e:02d} МСК'}")

@router.message(Command("notify"))
async def cmd_notify(m: Message, command: CommandObject):
    if not command.args:
        await m.answer("Формат: <code>/notify all|important|off</code>")
        return
    mode = command.args.strip().lower()
    if mode not in ("all", "important", "off"):
        await m.answer("Доступно: all / important / off")
        return
    await update_user_pref(m.chat.id, "notify_mode", mode)
    await refresh_user_prefs(m.chat.id)
    await m.answer(f"✅ Уведомления: {mode}")

# ---------- Callbacks ----------
@router.callback_query(F.data == "ctl:menu")
async def cb_menu(cq: CallbackQuery):
    accounts = await list_accounts(cq.from_user.id)
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text(
        f"👋 Аккаунты ({len(accounts)}/{MAX_ACCOUNTS_PER_USER}):",
        reply_markup=main_menu_kb(accounts, prefs))
    await cq.answer()

@router.callback_query(F.data == "ctl:startall")
async def cb_startall(cq: CallbackQuery):
    accounts = await list_accounts(cq.from_user.id)
    prefs = await get_user_prefs(cq.from_user.id)
    notify = _notify_for(cq.bot, cq.from_user.id)
    started = 0
    for a in accounts:
        if a.get("token") and not is_running(a["id"]):
            try:
                await register_battler(a, notify, prefs)
                started += 1
            except Exception:
                log.exception("start_all acc=%s", a["id"])
    await cq.answer(f"Запущено: {started}")
    await cb_menu(cq)

@router.callback_query(F.data == "ctl:stopall")
async def cb_stopall(cq: CallbackQuery):
    accounts = await list_accounts(cq.from_user.id)
    stopped = 0
    for a in accounts:
        if is_running(a["id"]):
            await stop_battler(a["id"])
            stopped += 1
    await cq.answer(f"Остановлено: {stopped}")
    await cb_menu(cq)

@router.callback_query(F.data == "ctl:user_settings")
async def cb_user_settings(cq: CallbackQuery):
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text("⚙️ Общие настройки:",
                                reply_markup=user_settings_kb(prefs))
    await cq.answer()

@router.callback_query(F.data == "uset:notify_menu")
async def cb_notify_menu(cq: CallbackQuery):
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text("🔔 Уведомления:",
                                reply_markup=notify_menu_kb(prefs["notify_mode"]))
    await cq.answer()

@router.callback_query(F.data.startswith("uset:notify:"))
async def cb_notify_set(cq: CallbackQuery):
    mode = cq.data.split(":")[2]
    if mode not in ("all", "important", "off"):
        await cq.answer("Неверный режим"); return
    await update_user_pref(cq.from_user.id, "notify_mode", mode)
    await refresh_user_prefs(cq.from_user.id)
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text("🔔 Уведомления:",
                                reply_markup=notify_menu_kb(prefs["notify_mode"]))
    await cq.answer(f"Режим: {mode}")

@router.callback_query(F.data == "uset:sched_menu")
async def cb_sched_menu(cq: CallbackQuery):
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text(
        f"🕒 Расписание катакомб (МСК)\n"
        f"Текущее: {'24/7' if prefs['schedule_start'] == 0 and prefs['schedule_end'] == 0 else f'{prefs["schedule_start"]:02d}–{prefs["schedule_end"]:02d}'}",
        reply_markup=schedule_menu_kb(prefs["schedule_start"], prefs["schedule_end"]))
    await cq.answer()

@router.callback_query(F.data.startswith("uset:sched:"))
async def cb_sched_set(cq: CallbackQuery):
    parts = cq.data.split(":")
    if len(parts) < 4:
        await cq.answer("Некорректно"); return
    try:
        s, e = int(parts[2]), int(parts[3])
    except ValueError:
        await cq.answer("Некорректно"); return
    await update_user_pref(cq.from_user.id, "schedule_start", s)
    await update_user_pref(cq.from_user.id, "schedule_end", e)
    await refresh_user_prefs(cq.from_user.id)
    prefs = await get_user_prefs(cq.from_user.id)
    await cq.message.edit_text(
        f"🕒 Расписание: {'24/7' if s == 0 and e == 0 else f'{s:02d}–{e:02d} МСК'}",
        reply_markup=schedule_menu_kb(prefs["schedule_start"], prefs["schedule_end"]))
    await cq.answer("Обновлено")

@router.callback_query(F.data == "uset:sched_help")
async def cb_sched_help(cq: CallbackQuery):
    await cq.answer("Отправьте в чат: /schedule 3 23", show_alert=True)

@router.callback_query(F.data == "acc:add")
async def cb_acc_add(cq: CallbackQuery):
    await cq.message.edit_text(
        "Добавьте аккаунт:\n"
        "<code>/add логин пароль [метка]</code>")
    await cq.answer()

@router.callback_query(F.data.startswith("acc:view:"))
async def cb_acc_view(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    status = "🟢 работает" if is_running(aid) else "🔴 остановлен"
    auto = "🔁 вкл" if a.get("auto_start") else "выкл"
    await cq.message.edit_text(
        f"👤 <b>{a['label']}</b> (ID {aid})\n"
        f"Статус: {status}\n"
        f"Автозапуск: {auto}\n"
        f"Рейд: {'вкл' if a['raid_enabled'] else 'выкл'} | "
        f"PvP: {'вкл' if a['pvp_enabled'] else 'выкл'} | "
        f"Катакомбы: {'вкл' if a['cata_enabled'] else 'выкл'}",
        reply_markup=account_kb(a))
    await cq.answer()

@router.callback_query(F.data.startswith("acc:start:"))
async def cb_acc_start(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    if not a.get("token"):
        await cq.answer("Нет токена. Удалите и добавьте заново.", show_alert=True); return
    if is_running(aid):
        await cq.answer("Уже запущен"); return
    prefs = await get_user_prefs(cq.from_user.id)
    await register_battler(a, _notify_for(cq.bot, cq.from_user.id), prefs)
    await cq.answer("Запущен")
    await cb_acc_view(cq)

@router.callback_query(F.data.startswith("acc:stop:"))
async def cb_acc_stop(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await stop_battler(aid)
    await cq.answer("Остановлен")
    await cb_acc_view(cq)

@router.callback_query(F.data.startswith("acc:autostart:"))
async def cb_acc_autostart(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    new_val = 0 if a["auto_start"] else 1
    await update_account_flag(aid, "auto_start", new_val)
    await cq.answer(f"Автозапуск: {'вкл' if new_val else 'выкл'}")
    await cb_acc_view(cq)

@router.callback_query(F.data.startswith("acc:settings:"))
async def cb_acc_settings(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await cq.message.edit_text(f"⚙️ «{a['label']}»:", reply_markup=settings_kb(a))
    await cq.answer()

@router.callback_query(F.data.startswith("acc:profile:"))
async def cb_acc_profile(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id or not a.get("token"):
        await cq.answer("Нет данных", show_alert=True); return
    cli = RemangaClient(a["token"], account_id=aid)
    try:
        p = await cli.profile() or {}
        if isinstance(p, dict) and p.get("_server_down"): p = {}
        ep = await cli.eventpoint_balance() or {}
        if isinstance(ep, dict) and ep.get("_server_down"): ep = {}
        st = await cli.cata_state() or {}
        if isinstance(st, dict) and st.get("_server_down"): st = {}
    finally:
        await cli.close()
    stars = int(p.get("awakening_energy", 0) or 0)
    buy_note = (f"⛔ ≥ {MAX_STARS_AUTO_BUY}" if stars >= MAX_STARS_AUTO_BUY
                else f"до {MAX_STARS_AUTO_BUY}")
    await cq.message.edit_text(
        f"👤 <b>{a['label']}</b>\n"
        f"⚡ {p.get('energy_current','?')}/{p.get('energy_max','?')}\n"
        f"⭐ {stars} ({buy_note})\n"
        f"🎲 {p.get('full_reroll_energy','?')}\n"
        f"💠 {p.get('reroll_dust','?')}\n"
        f"🎯 {ep.get('balance','?')}\n"
        f"🏆 {p.get('pvp_wins',0)}W / {p.get('pvp_losses',0)}L\n\n"
        f"{AutoBattler._resurrect_line(st)}",
        reply_markup=account_kb(a))
    await cq.answer()

@router.callback_query(F.data.startswith("acc:del:"))
async def cb_acc_del(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await stop_battler(aid)
    await delete_account(aid)
    await cq.answer("Удалён")
    await cb_menu(cq)

@router.callback_query(F.data.startswith("tgl:"))
async def cb_toggle(cq: CallbackQuery):
    _, aid_s, field = cq.data.split(":", 2)
    aid = int(aid_s)
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    new_val = 0 if a[field] else 1
    await update_account_flag(aid, field, new_val)
    was_running = is_running(aid)
    if was_running:
        await stop_battler(aid)
    a = await get_account(aid)
    if was_running:
        prefs = await get_user_prefs(cq.from_user.id)
        await register_battler(a, _notify_for(cq.bot, cq.from_user.id), prefs)
    await cq.message.edit_text(f"⚙️ «{a['label']}»:", reply_markup=settings_kb(a))
    await cq.answer("Обновлено")

@router.callback_query(F.data.startswith("raidloc:"))
async def cb_raidloc(cq: CallbackQuery):
    parts = cq.data.split(":")
    aid = int(parts[1])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await cq.message.edit_text("📍 Локация:", reply_markup=raid_loc_kb(aid, a["raid_loc"]))
    await cq.answer()

@router.callback_query(F.data.startswith("raidloc:set:"))
async def cb_raidloc_set(cq: CallbackQuery):
    _, _, aid_s, loc_s = cq.data.split(":")
    aid, loc = int(aid_s), int(loc_s)
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await update_account_flag(aid, "raid_loc", loc)
    a = await get_account(aid)
    await cq.message.edit_text(f"⚙️ «{a['label']}»:", reply_markup=settings_kb(a))
    await cq.answer(f"Локация {loc}")

@router.callback_query(F.data.startswith("cata:menu:"))
async def cb_cata_menu(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(a))
    await cq.answer()

@router.callback_query(F.data.startswith("cata:auto:"))
async def cb_cata_auto(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    await update_account_flag(aid, "cata_fixed", 0)
    a = await get_account(aid)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(a))
    await cq.answer("Авто")

@router.callback_query(F.data.startswith("cata:fixed:"))
async def cb_cata_fixed(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    await update_account_flag(aid, "cata_fixed", 1)
    a = await get_account(aid)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(a))
    await cq.answer("Фикс.")

@router.callback_query(F.data.startswith("cata:lvl:"))
async def cb_cata_lvl(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await cq.message.edit_text("📍 Уровень (1–25):", reply_markup=cata_lvl_kb(aid, a["cata_level"]))
    await cq.answer()

@router.callback_query(F.data.startswith("cata:setlvl:"))
async def cb_cata_setlvl(cq: CallbackQuery):
    _, _, aid_s, lvl_s = cq.data.split(":")
    aid, lvl = int(aid_s), int(lvl_s)
    await update_account_flag(aid, "cata_level", lvl)
    a = await get_account(aid)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(a))
    await cq.answer(f"Уровень {lvl}")

@router.callback_query(F.data.startswith("cata:stars:"))
async def cb_cata_stars(cq: CallbackQuery):
    aid = int(cq.data.split(":")[2])
    a = await get_account(aid)
    if not a or a["chat_id"] != cq.from_user.id:
        await cq.answer("Нет доступа", show_alert=True); return
    await cq.message.edit_text("⭐ Сложность:", reply_markup=cata_stars_kb(aid, a["cata_stars"]))
    await cq.answer()

@router.callback_query(F.data.startswith("cata:setstars:"))
async def cb_cata_setstars(cq: CallbackQuery):
    _, _, aid_s, s_s = cq.data.split(":")
    aid, s = int(aid_s), int(s_s)
    await update_account_flag(aid, "cata_stars", s)
    a = await get_account(aid)
    await cq.message.edit_text("🏺 Катакомбы:", reply_markup=cata_menu_kb(a))
    await cq.answer(f"{s}★")

# ============================================================
# HEALTH + ЗАПУСК
# ============================================================
async def _health(_): return web.Response(text="ok")

async def start_health_server():
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT); await site.start()
    log.info("Health on :%d", PORT)
    return runner

def make_bot() -> Bot:
    conn = TCPConnector(family=socket.AF_INET)
    sess = AiohttpSession(); sess._connector = conn
    return Bot(token=BOT_TOKEN,
               default=DefaultBotProperties(parse_mode=ParseMode.HTML),
               session=sess, timeout=120)

async def main():
    await init_db()
    health = await start_health_server()
    asyncio.create_task(_rate_limit_watcher())

    bot = make_bot()

    # Автозапуск аккаунтов
    asyncio.create_task(_autostart_accounts(bot))
    # Сводный отчёт раз в час
    asyncio.create_task(_hourly_report_loop(bot))

    dp = Dispatcher()
    dp.include_router(router)
    log.info("Bot polling started")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for a in await all_accounts_for_startup():
            await stop_battler(a["id"])
        await bot.session.close()
        with suppress(Exception): await health.cleanup()
        await close_pool()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
