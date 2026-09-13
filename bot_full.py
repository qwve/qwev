#!/usr/bin/env python3
"""
================================================================================
 Instagram Account Status Monitor — Telegram Bot (Advanced)
================================================================================

Feature parity with the Discord "Advanced" bot, adapted to Telegram's DM-only
model (no channel routing — everything happens in the bot chat itself, as
requested):

  - Instagram-style stat cards (real avatar, Follow/Message pills, posts /
    followers / following, verified checkmark, muted "UserNotFound" card for
    bans / not-found lookups)
  - Bulk adding (unchanged from before — one username per line or comma-sep)
  - Verification monitoring — OPT-IN per account via a dedicated "🔐 Verify
    Add" flow, separate from the normal Add flow, so adding a verified
    account for ban/unban monitoring never fires a surprise "verified" alert
  - Redis-backed persistence (Upstash free tier) so data survives
    restarts/redeploys on hosts with no persistent disk (e.g. Render free
    tier) — falls back to local JSON files automatically if unset
  - "Account has been wiped" / "Account has returned from the grave" premium
    phrasing, time-taken on every alert type, custom/premium Telegram emoji
    support, branded footer
  - 🎭 Fake Alert — test-only rendering of any alert type, pulling a real
    account's current stats/avatar (same as the actual alert would show), so
    you can see the format without needing a real ban/unban/verification event

DETECTION METHOD / KNOWN LIMITATIONS: unchanged from before — see the
original notes retained below.

KNOWN LIMITATION (please read):
  InstaNavigation's backend appears to cache profile data server-side,
  shared across all callers (not per-visitor). In testing, this caused
  roughly a ~2 hour delay between a real status change and this bot
  detecting it. This is a limitation of the underlying data source, not
  something fixable in this code — there is no cache-busting parameter
  available in their API.

HOW TO RUN LOCALLY:
  pip install -r requirements.txt
  cp .env.example .env   (fill in BOT_TOKEN, AUTHORIZED_CHAT_IDS)
  python bot_full.py

HOW TO DEPLOY (Docker / Render): unchanged — see Dockerfile.
================================================================================
"""

import os
import re
import io
import json
import uuid
import asyncio
import logging
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError, Forbidden, BadRequest

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "20"))
CONFIRMATION_THRESHOLD = int(os.getenv("CONFIRMATION_THRESHOLD", "2"))
PORT = int(os.getenv("PORT", "8080"))

DATA_FILE = Path(os.getenv("DATA_FILE", "monitored_accounts.json"))
CARDS_DIR = Path(os.getenv("CARDS_DIR", "cards"))
CARDS_DIR.mkdir(parents=True, exist_ok=True)

BOT_FOOTER_TEXT = os.getenv("BOT_FOOTER_TEXT", "Instagram Monitor — Premium Monitoring").strip()

# ----------------------------------------------------------------------------
# Proxy pool (see "PROXY POOL" section below for the fetch/health-check logic)
# ----------------------------------------------------------------------------
# Manually-supplied proxies (comma-separated http://[user:pass@]host:port).
# Optional — these are merged with the auto-fetched free proxies below.
PROXIES_STATIC = [p.strip() for p in os.getenv("PROXIES", "").split(",") if p.strip()]

# Auto-fetch free public proxies from a live list and health-check them
# periodically instead of relying on a static, quickly-stale list.
PROXY_AUTO_FETCH = os.getenv("PROXY_AUTO_FETCH", "true").strip().lower() not in ("false", "0", "no")
PROXY_SOURCE_URL = os.getenv(
    "PROXY_SOURCE_URL",
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
).strip()
PROXY_REFRESH_INTERVAL_SECONDS = int(os.getenv("PROXY_REFRESH_INTERVAL_SECONDS", "600"))
PROXY_HEALTHCHECK_URL = os.getenv("PROXY_HEALTHCHECK_URL", "http://httpbin.org/ip").strip()
PROXY_HEALTHCHECK_TIMEOUT_SECONDS = int(os.getenv("PROXY_HEALTHCHECK_TIMEOUT_SECONDS", "6"))
PROXY_HEALTHCHECK_CONCURRENCY = int(os.getenv("PROXY_HEALTHCHECK_CONCURRENCY", "30"))
PROXY_POOL_MAX = int(os.getenv("PROXY_POOL_MAX", "30"))

INTER_CHECK_DELAY_MS = 2000
MAX_BULK_ADD = 25  # safety cap so one paste can't queue an unbounded number of checks

API_URL = "https://insta-story.com/api/v1/web/profile"

if not BOT_TOKEN:
    raise SystemExit("ERROR: BOT_TOKEN is not set in .env")

# ============================================================================
# AUTHORIZATION WITH TIME-LIMITED ACCESS (unchanged)
# ============================================================================
def parse_duration(duration_str: str) -> Optional[timedelta]:
    match = re.fullmatch(r"(\d+)([dhm])", duration_str.strip())
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "m":
        return timedelta(minutes=value)
    return None

_raw_chat_ids = os.getenv("AUTHORIZED_CHAT_IDS", "").strip()
_bot_start_time = datetime.now(timezone.utc)

AUTHORIZED_USERS: dict = {}
for entry in _raw_chat_ids.split(","):
    entry = entry.strip()
    if not entry:
        continue
    if ":" in entry:
        uid_str, duration_str = entry.split(":", 1)
        duration = parse_duration(duration_str)
        AUTHORIZED_USERS[int(uid_str)] = (_bot_start_time + duration) if duration else None
    else:
        AUTHORIZED_USERS[int(entry)] = None

if not AUTHORIZED_USERS:
    raise SystemExit("ERROR: AUTHORIZED_CHAT_IDS is not set in .env")

def is_authorized(user_id: int) -> bool:
    if user_id not in AUTHORIZED_USERS:
        return False
    expiry = AUTHORIZED_USERS[user_id]
    if expiry is None:
        return True
    return datetime.now(timezone.utc) < expiry

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("telegram").setLevel(logging.WARNING)
logger = logging.getLogger("igbot")

# ============================================================================
# PERSISTENCE — Upstash Redis (free tier, survives restarts/redeploys) with
# automatic fallback to local JSON files if the env vars aren't set. Same
# approach as the Discord bots use, so data reliability isn't tied to
# whichever host has a persistent disk available.
# ============================================================================
_storage_lock = asyncio.Lock()

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "").strip()
USE_REDIS = bool(UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN)
ACCOUNTS_REDIS_KEY = os.getenv("ACCOUNTS_REDIS_KEY", "igbot:monitored_accounts")

if USE_REDIS:
    logger.info("Persistence backend: Upstash Redis")
else:
    logger.warning(
        "Persistence backend: local JSON files (UPSTASH_REDIS_REST_URL/TOKEN "
        "not set). On a host with no persistent disk (e.g. Render free tier) "
        "this data will NOT survive a restart or redeploy."
    )

def _validate_data(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    for user_key, user_accounts in data.items():
        if not isinstance(user_key, str) or not user_key.startswith("user_"):
            return False
        if not isinstance(user_accounts, dict):
            return False
        for account_key, entry in user_accounts.items():
            if not isinstance(entry, dict):
                return False
            for field in ["username", "status", "last_checked", "case_index"]:
                if field not in entry:
                    return False
    return True

async def _redis_command(*args) -> Optional[dict]:
    headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(UPSTASH_REDIS_REST_URL, json=list(args), headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Redis command failed ({resp.status}): {body[:200]}")
                    return None
                return await resp.json()
    except Exception as e:
        logger.error(f"Redis command error: {e}")
        return None

async def _redis_get_json(key: str) -> dict:
    result = await _redis_command("GET", key)
    if not result or result.get("result") is None:
        return {}
    try:
        return json.loads(result["result"])
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Corrupt JSON in Redis key {key}, starting fresh.")
        return {}

async def _redis_set_json(key: str, data: dict) -> bool:
    result = await _redis_command("SET", key, json.dumps(data, ensure_ascii=False))
    ok = bool(result and result.get("result") == "OK")
    if not ok:
        logger.error(f"Failed to save Redis key {key}")
    return ok

def _load_data_file() -> dict:
    if not DATA_FILE.exists():
        logger.info(f"Data file {DATA_FILE} does not exist yet.")
        return {}
    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded data file with {len(data)} users.")
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Failed to load data file: {e}. Starting fresh.")
        return {}

def _save_data_file(data: dict) -> None:
    tmp_path = DATA_FILE.with_suffix(".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp_path.replace(DATA_FILE)
        logger.info(f"Successfully saved data file with {len(data)} users.")
    except OSError as e:
        logger.error(f"Failed to save data file: {e}")

async def load_data() -> dict:
    async with _storage_lock:
        if USE_REDIS:
            return await _redis_get_json(ACCOUNTS_REDIS_KEY)
        return _load_data_file()

async def save_data(data: dict) -> None:
    async with _storage_lock:
        if not _validate_data(data):
            logger.error("Data validation failed! Not saving to prevent corruption.")
            return
        if USE_REDIS:
            await _redis_set_json(ACCOUNTS_REDIS_KEY, data)
        else:
            _save_data_file(data)

def get_user_key(user_id: int) -> str:
    return f"user_{user_id}"

# ============================================================================
# HELPER: Human-Readable Time (unchanged)
# ============================================================================
def relative_time(iso_timestamp: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        now = datetime.now(timezone.utc)
        delta = now - dt
        if delta.total_seconds() < 60:
            return "just now"
        elif delta.total_seconds() < 3600:
            minutes = int(delta.total_seconds() // 60)
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
        elif delta.total_seconds() < 86400:
            hours = int(delta.total_seconds() // 3600)
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            days = int(delta.total_seconds() // 86400)
            return f"{days} day{'s' if days > 1 else ''} ago"
    except Exception:
        return "unknown"

def format_username_link(username: str) -> str:
    return f'<a href="https://instagram.com/{username}">@{username}</a>'

def format_duration_hm(seconds: float) -> str:
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m"

def compute_time_taken(entry: dict) -> Optional[str]:
    added_at_str = entry.get("added_at")
    if not added_at_str:
        return None
    try:
        added_at = datetime.fromisoformat(added_at_str)
        elapsed = (datetime.now(timezone.utc) - added_at).total_seconds()
        return format_duration_hm(elapsed)
    except Exception:
        return None

def count_alpha_chars(username: str) -> int:
    return sum(1 for c in username if c.isalpha())

def generate_case_variant(original_username: str, case_index: int) -> str:
    letters = count_alpha_chars(original_username)
    if letters == 0:
        return original_username
    total = 1 << letters
    case_index %= total
    result = []
    bit_pos = 0
    for ch in original_username:
        if ch.isalpha():
            if (case_index >> bit_pos) & 1:
                result.append(ch.upper())
            else:
                result.append(ch.lower())
            bit_pos += 1
        else:
            result.append(ch)
    return "".join(result)

def next_case_index(original_username: str, current_index: int) -> int:
    letters = count_alpha_chars(original_username)
    if letters == 0:
        return 0
    total = 1 << letters
    return (current_index + 1) % total

# ============================================================================
# CUSTOM / PREMIUM EMOJI (optional)
#
# EMOJI_<KEY> env var can hold either a plain unicode emoji (used as-is) or
# a numeric Telegram custom emoji ID (wrapped in <tg-emoji>, so people with
# Telegram Premium see your custom emoji and everyone else sees the
# fallback unicode automatically — no setup required to work either way).
# To get a custom emoji's ID: forward/send it in a chat with @userinfobot or
# similar, or use the emoji in a message to a bot that echoes entities.
# ============================================================================
EMOJI_DEFAULTS = {
    "verified": "🔵",
    "trophy": "🏆",
    "clock": "⏰",
    "skull": "💀",
    "grave": "⚰️",
    "warning": "⚠️",
}

def _emoji_html(key: str) -> str:
    raw = os.getenv(f"EMOJI_{key.upper()}", "").strip()
    fallback = EMOJI_DEFAULTS[key]
    if not raw:
        return fallback
    if raw.isdigit():
        return f'<tg-emoji emoji-id="{raw}">{fallback}</tg-emoji>'
    return raw

EMOJI = {k: _emoji_html(k) for k in EMOJI_DEFAULTS}

# ============================================================================
# STAT CARD IMAGE GENERATION — Instagram-style dark profile card
# ============================================================================
def _find_font(candidates: list, fallback: str) -> str:
    for path in candidates:
        if os.path.exists(path):
            return path
    return fallback

_DEJAVU_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
_DEJAVU_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

# Inter is installed via apt-get (see Dockerfile) — matches the Discord
# Advanced bot's card look exactly. Falls back through Roboto, then
# DejaVu, so this never crashes on a bare host.
FONT_BOLD = _find_font([
    "/usr/share/fonts/opentype/inter/Inter-Bold.otf",
    "/usr/share/fonts/truetype/roboto/unhinted/RobotoTTF/Roboto-Bold.ttf",
    "/usr/share/fonts/truetype/roboto-fontface/roboto/Roboto-Bold.ttf",
], _DEJAVU_BOLD)
FONT_REGULAR = _find_font([
    "/usr/share/fonts/opentype/inter/Inter-Regular.otf",
    "/usr/share/fonts/truetype/roboto/unhinted/RobotoTTF/Roboto-Regular.ttf",
    "/usr/share/fonts/truetype/roboto-fontface/roboto/Roboto-Regular.ttf",
], _DEJAVU_REGULAR)

def format_count(n) -> str:
    if n is None:
        return "0"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".replace(".0K", "K")
    return str(n)

def _center_crop_square(img: Image.Image) -> Image.Image:
    """Crop the longer side down so the image is square before it goes into
    the circular mask — avoids squishing non-square avatars."""
    w, h = img.size
    if w == h:
        return img
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    return img.crop((left, top, left + side, top + side))

def make_circular(img: Image.Image, size: int) -> Image.Image:
    img = _center_crop_square(img.convert("RGBA")).resize((size, size), Image.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    d = ImageDraw.Draw(mask)
    d.ellipse((0, 0, size, size), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img, (0, 0), mask)
    return out

# ============================================================================
# PROXY POOL
# ============================================================================
# Free public proxy lists die fast (often within hours), so instead of a
# fixed list we periodically pull a fresh candidate list from PROXY_SOURCE_URL,
# health-check each candidate with a cheap request, and only rotate across
# ones that actually responded. PROXIES_STATIC (from the PROXIES env var) is
# merged in as extra candidates so manually-supplied proxies get used too.
# If PROXY_AUTO_FETCH is off, or the fetch/health-check fails, we just fall
# back to PROXIES_STATIC as-is (unchecked), and to no proxy if that's empty.
_live_proxy_pool: list = list(PROXIES_STATIC)

def _pick_proxy() -> Optional[str]:
    return random.choice(_live_proxy_pool) if _live_proxy_pool else None

async def _fetch_proxy_candidates() -> list:
    """Pull a fresh list of free proxy candidates from PROXY_SOURCE_URL."""
    candidates = list(PROXIES_STATIC)
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(PROXY_SOURCE_URL) as resp:
                if resp.status != 200:
                    logger.warning(f"Proxy source fetch got HTTP {resp.status}")
                    return candidates
                text = await resp.text()
        for line in text.splitlines():
            line = line.strip()
            if not line or "." not in line or ":" not in line:
                continue
            candidates.append(line if "://" in line else f"http://{line}")
    except Exception as e:
        logger.warning(f"Failed to fetch proxy source list: {e}")
    # de-dupe, keep order
    seen = set()
    deduped = []
    for p in candidates:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped

async def _proxy_is_alive(session: aiohttp.ClientSession, proxy: str, sem: asyncio.Semaphore) -> Optional[str]:
    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=PROXY_HEALTHCHECK_TIMEOUT_SECONDS)
            async with session.get(PROXY_HEALTHCHECK_URL, proxy=proxy, timeout=timeout) as resp:
                if resp.status == 200:
                    return proxy
        except Exception:
            pass
        return None

async def refresh_proxy_pool() -> None:
    """Fetch fresh candidates, health-check them concurrently, and replace
    the live pool with the ones that responded (capped at PROXY_POOL_MAX)."""
    global _live_proxy_pool

    if not PROXY_AUTO_FETCH:
        _live_proxy_pool = list(PROXIES_STATIC)
        return

    start = asyncio.get_event_loop().time()
    candidates = await _fetch_proxy_candidates()
    if not candidates:
        logger.warning("No proxy candidates found; keeping previous pool.")
        return
    logger.info(f"Proxy pool: fetched {len(candidates)} candidates, health-checking...")

    sem = asyncio.Semaphore(PROXY_HEALTHCHECK_CONCURRENCY)
    try:
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[_proxy_is_alive(session, p, sem) for p in candidates],
                return_exceptions=False,
            )
    except Exception as e:
        logger.warning(f"Proxy health-check pass failed: {e}")
        return

    elapsed = asyncio.get_event_loop().time() - start
    alive = [p for p in results if p][:PROXY_POOL_MAX]
    if alive:
        _live_proxy_pool = alive
        logger.info(f"Proxy pool refreshed: {len(alive)}/{len(candidates)} candidates alive ({elapsed:.1f}s).")
    else:
        logger.warning(f"Proxy health-check found 0/{len(candidates)} alive ({elapsed:.1f}s); keeping previous pool.")


async def proxy_refresh_loop() -> None:
    while True:
        try:
            await refresh_proxy_pool()
        except Exception as e:
            logger.exception(f"Proxy pool refresh error: {e}")
        await asyncio.sleep(PROXY_REFRESH_INTERVAL_SECONDS)

# Shared browser-identity headers for every request to insta-story.com (the
# API) AND the Instagram CDN image URLs it hands back — both are guarded by
# the same anti-bot checks, so both need to look like the same "browser
# session" hitting insta-story.com.
IG_REQUEST_HEADERS = {
    "Origin": "https://insta-story.com",
    "Referer": "https://insta-story.com/instanavigation",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

async def fetch_profile_pic_bytes(profile_pic_url: Optional[str]) -> Optional[bytes]:
    if not profile_pic_url:
        return None
    headers = {
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        **IG_REQUEST_HEADERS,
    }
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(profile_pic_url, headers=headers, proxy=_pick_proxy()) as resp:
                if resp.status == 200:
                    return await resp.read()
                logger.warning(f"Profile picture fetch got HTTP {resp.status}")
    except Exception as e:
        logger.warning(f"Failed to fetch profile picture: {e}")
    return None

# Sampled directly from the Discord Advanced bot's card — a soft charcoal,
# not pure black.
CARD_BG = (18, 18, 20)

def _pill_centered(d: ImageDraw.ImageDraw, x, y_center, w, h, color, text, font_size=26):
    """Vertically-centered pill, used in the single-row header below."""
    top = y_center - h / 2
    d.rounded_rectangle((x, top, x + w, top + h), radius=h / 2, fill=color)
    f = ImageFont.truetype(FONT_BOLD, font_size)
    d.text((x + w / 2, y_center), text, font=f, fill="white", anchor="mm")

def _draw_verified_badge(d: ImageDraw.ImageDraw, cx, cy, r=16, color=(0, 149, 246), outline=None):
    outline = outline or CARD_BG
    d.ellipse((cx - r - 3, cy - r - 3, cx + r + 3, cy + r + 3), fill=outline)
    d.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color)
    d.line(
        [(cx - r * 0.5, cy + r * 0.05), (cx - r * 0.05, cy + r * 0.45), (cx + r * 0.55, cy - r * 0.35)],
        fill="white", width=3, joint="curve",
    )

def _draw_dots(d: ImageDraw.ImageDraw, x, y, color=(140, 140, 140), r=4, gap=15):
    for i in range(3):
        cx = x + i * gap
        d.ellipse((cx - r, y - r, cx + r, y + r), fill=color)
    return x + 2 * gap + r

def _draw_stat(d: ImageDraw.ImageDraw, x, y, number, label, font_num, font_label, gap_after=44):
    d.text((x, y), number, font=font_num, fill="white", anchor="lm")
    num_w = d.textlength(number, font=font_num)
    label_x = x + num_w + 8
    d.text((label_x, y), label, font=font_label, fill=(163, 163, 163), anchor="lm")
    label_w = d.textlength(label, font=font_label)
    return label_x + label_w + gap_after

STATUS_PILL_COLORS = {
    "RECOVERED": (56, 193, 114),
    "BANNED": (224, 54, 54),
    "VERIFIED": (0, 149, 246),
    "EXPIRED": (230, 160, 30),
}
RING_COLOR_DEFAULT = (70, 70, 70)
RING_COLOR_GREEN = (56, 193, 114)
RING_COLOR_RED = (224, 54, 54)
RING_COLOR_BLUE = (0, 149, 246)
RING_COLOR_YELLOW = (230, 160, 30)
CARD_CORNER_RADIUS = 40

def _round_corners(img: Image.Image, radius: int) -> Image.Image:
    img = img.convert("RGBA")
    mask = Image.new("L", img.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, img.width, img.height), radius=radius, fill=255)
    out = Image.new("RGBA", img.size, (0, 0, 0, 0))
    out.paste(img, (0, 0), mask)
    return out

# Layout constants — identical to the Discord Advanced bot's card, so both
# platforms render the same alert the same way.
W, H = 1170, 453
AX, AY, ASZ = 60, 95, 280
RING_PAD, RING_WIDTH = 6, 7
TX = 379
ROW1_Y = 148
ROW2_Y = 246
ROW3_Y = 309

def generate_stat_card(username, status, followers=None, following=None, posts=None,
                        profile_pic_bytes=None, is_verified=False,
                        status_label=None, ring_color=None, bio=None) -> str:
    """
    Two variants, styled after Instagram's dark-mode profile header — same
    layout as the Discord Advanced bot's card:
      - "active": avatar, live stats, one header row (username + optional
        verified badge + colored status pill + "..." menu), a stats row,
        and an optional bio line — used for RECOVERED (green), VERIFIED
        (blue), EXPIRED (yellow) cards.
      - anything else ("suspended" / not found): muted gray avatar with a
        red X overlay, username forced to "UserNotFound", stats forced to
        0/0/0.

    status_label: "RECOVERED" / "BANNED" / "VERIFIED" / "EXPIRED" — picks
    the pill color/text from STATUS_PILL_COLORS. Falls back to a plain
    blue "Follow" pill if omitted.

    ring_color: overrides the avatar ring color. Pass one of the
    RING_COLOR_* constants to match status_label.
    """
    img = Image.new("RGB", (W, H), CARD_BG)
    d = ImageDraw.Draw(img)
    fb = ImageFont.truetype(FONT_BOLD, 46)
    fr = ImageFont.truetype(FONT_REGULAR, 26)
    fs = ImageFont.truetype(FONT_BOLD, 34)

    ring = ring_color or RING_COLOR_DEFAULT
    d.ellipse(
        (AX - RING_PAD, AY - RING_PAD, AX + ASZ + RING_PAD, AY + ASZ + RING_PAD),
        outline=ring, width=RING_WIDTH,
    )

    pill_color = (0, 149, 246)
    pill_text = "Follow"
    if status_label and status_label in STATUS_PILL_COLORS:
        pill_color = STATUS_PILL_COLORS[status_label]
        pill_text = status_label

    if status != "active":
        d.ellipse((AX, AY, AX + ASZ, AY + ASZ), fill=(80, 80, 80))
        pad = ASZ * 0.2
        d.line((AX + pad, AY + pad, AX + ASZ - pad, AY + ASZ - pad), fill=(214, 45, 45), width=14)
        d.line((AX + ASZ - pad, AY + pad, AX + pad, AY + ASZ - pad), fill=(214, 45, 45), width=14)

        d.text((TX, ROW1_Y), "UserNotFound", font=fb, fill="white", anchor="lm")
        pill_w = max(160, len(pill_text) * 17 + 50)
        _pill_centered(d, TX, ROW1_Y + 62, pill_w, 54, pill_color, pill_text)

        d.text((TX, ROW2_Y), "0 posts", font=fr, fill=(163, 163, 163))
        d.text((TX + 190, ROW2_Y), "0 followers", font=fr, fill=(163, 163, 163))
        d.text((TX + 440, ROW2_Y), "0 following", font=fr, fill=(163, 163, 163))
        d.text((TX, ROW3_Y), "UserNotFound", font=fr, fill=(130, 130, 130))

        p = CARDS_DIR / f"card_{username}_{status}.png"
        _round_corners(img, CARD_CORNER_RADIUS).save(p)
        return str(p)

    if profile_pic_bytes:
        try:
            av = make_circular(Image.open(io.BytesIO(profile_pic_bytes)), ASZ)
            img.paste(av, (AX, AY), av)
        except Exception:
            d.ellipse((AX, AY, AX + ASZ, AY + ASZ), fill=(60, 60, 60))
    else:
        d.ellipse((AX, AY, AX + ASZ, AY + ASZ), fill=(60, 60, 60))

    d.text((TX, ROW1_Y), username, font=fb, fill="white", anchor="lm")
    x = TX + d.textlength(username, font=fb) + 20

    if is_verified:
        _draw_verified_badge(d, x + 16, ROW1_Y - 2, r=16)
        x += 32 + 24

    pill_w = max(150, len(pill_text) * 17 + 50)
    _pill_centered(d, x, ROW1_Y, pill_w, 54, pill_color, pill_text)
    x += pill_w + 30

    _draw_dots(d, x, ROW1_Y)

    x = TX
    x = _draw_stat(d, x, ROW2_Y, str(posts or 0), "posts", fs, fr)
    x = _draw_stat(d, x, ROW2_Y, format_count(followers), "followers", fs, fr)
    _draw_stat(d, x, ROW2_Y, format_count(following), "following", fs, fr)

    if bio:
        d.text((TX, ROW3_Y), bio, font=fr, fill=(163, 163, 163), anchor="lm")

    tag = (status_label or status).lower()
    p = CARDS_DIR / f"card_{username}_{tag}.png"
    _round_corners(img, CARD_CORNER_RADIUS).save(p)
    return str(p)

# Maps each alert event to its card's (status_label, ring_color) — shared
# by the real poll-cycle alerts and /fake, so both render identically, and
# matches the Discord Advanced bot's EVENT_CARD_STYLE exactly.
EVENT_CARD_STYLE = {
    "ban": ("BANNED", RING_COLOR_RED),
    "unban": ("RECOVERED", RING_COLOR_GREEN),
    "verify_on": ("VERIFIED", RING_COLOR_BLUE),
    "verify_off": ("EXPIRED", RING_COLOR_YELLOW),
}

# ============================================================================
# INSTAGRAM STATUS CHECKER (direct API call, no browser needed)
# ============================================================================
class CheckResult:
    def __init__(self, status: str, followers: Optional[int] = None,
                 following: Optional[int] = None, posts: Optional[int] = None,
                 is_verified: bool = False, profile_pic_url: Optional[str] = None, note: str = ""):
        self.status = status  # "active" or "suspended" only
        self.followers = followers
        self.following = following
        self.posts = posts
        self.is_verified = is_verified
        self.profile_pic_url = profile_pic_url
        self.note = note

async def check_instagram_status(username: str, retries: int = 2) -> CheckResult:
    payload = {
        "username": username,
        "visitor_id": str(uuid.uuid4()),
        "user_info": True,
        "user_stories": False,
        "user_highlights": False,
        "user_posts": False,
    }
    headers = {
        "Content-Type": "application/json",
        **IG_REQUEST_HEADERS,
    }

    for attempt in range(retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(API_URL, json=payload, headers=headers, proxy=_pick_proxy()) as resp:
                    status_code = resp.status

                    if status_code == 429:
                        logger.warning(f"@{username}: rate-limited (attempt {attempt+1})")
                        if attempt < retries:
                            await asyncio.sleep(3)
                            continue
                        return CheckResult("suspended", note="rate-limited after retries")

                    try:
                        data = await resp.json()
                    except Exception:
                        logger.warning(f"@{username}: non-JSON response (attempt {attempt+1})")
                        if attempt < retries:
                            await asyncio.sleep(2)
                            continue
                        return CheckResult("suspended", note="non-JSON response after retries")

                    user_info = data.get("user_info")

                    if isinstance(user_info, dict) and user_info.get("id"):
                        followers = user_info.get("followers")
                        following = user_info.get("following")
                        posts = user_info.get("posts")
                        is_verified = bool(user_info.get("is_verified", False))
                        profile_pic_url = user_info.get("profile_pic_url")
                        logger.info(f"@{username}: ACTIVE (id={user_info.get('id')})")
                        return CheckResult("active", followers, following, posts, is_verified, profile_pic_url, note="")

                    logger.info(f"@{username}: SUSPENDED (no valid user_info in response)")
                    return CheckResult("suspended", note="")

        except asyncio.TimeoutError:
            logger.warning(f"@{username}: request timeout (attempt {attempt+1})")
            if attempt < retries:
                await asyncio.sleep(2)
                continue
            return CheckResult("suspended", note="timeout after retries")
        except Exception as e:
            logger.error(f"@{username}: request error (attempt {attempt+1}): {e}")
            if attempt < retries:
                await asyncio.sleep(2)
                continue
            return CheckResult("suspended", note="")

    return CheckResult("suspended", note="max retries exceeded")

# ============================================================================
# KEYBOARD HELPERS
# ============================================================================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("➕ Add"), KeyboardButton("📥 Bulk Add")],
            [KeyboardButton("🔐 Verify Add"), KeyboardButton("🔍 Check")],
            [KeyboardButton("📋 WatchList"), KeyboardButton("🎭 Fake Alert")],
            [KeyboardButton("🗑️ Clear All")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )

def removal_keyboard(username_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes", callback_data=f"remove_yes:{username_key}"),
            InlineKeyboardButton("❌ No", callback_data=f"remove_no:{username_key}"),
        ]
    ])

def list_item_keyboard(username_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑️ Remove this account", callback_data=f"remove_yes:{username_key}")]
    ])

def fake_alert_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 Ban Alert", callback_data="fake:ban"),
         InlineKeyboardButton("🟢 Unban Alert", callback_data="fake:unban")],
        [InlineKeyboardButton("🔵 Verified", callback_data="fake:verify_on"),
         InlineKeyboardButton("⚠️ Verify Expired", callback_data="fake:verify_off")],
    ])

# ============================================================================
# IMAGE HELPERS
# ============================================================================
async def send_card_with_caption(chat_id: int, app: Application, image_path: str,
                                   caption: str, reply_markup=None) -> None:
    try:
        with open(image_path, "rb") as img:
            await app.bot.send_photo(
                chat_id=chat_id, photo=img, caption=caption,
                parse_mode=ParseMode.HTML, reply_markup=reply_markup,
            )
    except Exception as e:
        logger.error(f"Failed to send card image {image_path}: {e}")
        await app.bot.send_message(
            chat_id=chat_id, text=caption, parse_mode=ParseMode.HTML, reply_markup=reply_markup
        )

# ============================================================================
# RESPONSE TEXT BUILDERS
# ============================================================================
def build_check_text(username: str, result: "CheckResult") -> str:
    """Matches the Discord Advanced bot's build_check_embed exactly:
    plain "@username" + a single "Followers: N | [verified badge]" line,
    or a "not found" line for a suspended/unresolvable account."""
    link = format_username_link(username)
    if result.status == "active":
        desc = [f"Followers: {result.followers:,}" if result.followers is not None else "Followers: N/A"]
        if result.is_verified:
            desc.append(EMOJI["verified"])
        lines = [f"<b>{link}</b>", " | ".join(desc)]
    else:
        lines = [f"{EMOJI['warning']} @{username} not found"]
    lines.append("")
    lines.append(f"<i>{BOT_FOOTER_TEXT}</i>")
    return "\n".join(lines)

def build_added_text(username: str, result: "CheckResult") -> str:
    """Matches the Discord Advanced bot's build_added_embed exactly."""
    link = format_username_link(username)
    if result.status == "active":
        desc = [f"Followers: {result.followers:,}" if result.followers is not None else "Followers: N/A"]
        if result.is_verified:
            desc.append(EMOJI["verified"])
        lines = [f"📋 <b>Added</b> {link} <b>to WatchList</b>", " | ".join(desc)]
    else:
        lines = [f"{EMOJI['warning']} @{username} not found"]
    lines.append("")
    lines.append(f"<i>{BOT_FOOTER_TEXT}</i>")
    return "\n".join(lines)

def build_event_text(event: str, username: str, followers=None, time_taken_str=None, is_verified=False) -> str:
    """event: 'ban' | 'unban' | 'verify_on' | 'verify_off' — matches the
    Discord Advanced bot's build_event_embed wording/format exactly:
    "<emoji> Account <Status> | <code>@username</code>" title, with bold
    values in the body."""
    status_words = {
        "ban": "Account Banned",
        "unban": "Account Recovered",
        "verify_on": "Account Verified",
        "verify_off": "Verification Expired",
    }
    status_emojis = {
        "ban": "❌",
        "unban": "✅",
        "verify_on": "🔵",
        "verify_off": EMOJI["warning"],
    }
    title = f"{status_emojis[event]} <b>{status_words[event]}</b> | <code>@{username}</code>"
    lines = [title]

    desc_parts = []
    if event in ("unban", "verify_on", "verify_off") and followers is not None:
        desc_parts.append(f"👥 Followers: <b>{followers:,}</b>")
    if time_taken_str:
        desc_parts.append(f"{EMOJI['clock']} Time: <b>{time_taken_str}</b>")
    if desc_parts:
        lines.append("")
        lines.append(" | ".join(desc_parts))

    lines.append("")
    lines.append(f"<i>{BOT_FOOTER_TEXT}</i>")
    return "\n".join(lines)

# ============================================================================
# COMMAND HANDLERS
# ============================================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update.effective_user.id):
        await update.effective_message.reply_text("⛔ You are not authorized.")
        return
    text = "📱 <b>Instagram Monitor Bot</b>\n\nUse the buttons below to get started:"
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)

# ============================================================================
# MESSAGE HANDLER
# ============================================================================
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    if not is_authorized(user_id):
        await update.effective_message.reply_text("⛔ Not authorized (or your access has expired).")
        return

    text = update.effective_message.text.strip()

    if text == "➕ Add":
        await update.effective_message.reply_text(
            "Send me the Instagram username you want to monitor (e.g., nasa)\n\n"
            "This will track ban/unban only. Use 🔐 Verify Add if you also want "
            "verification-status alerts for this account.",
            reply_markup=main_menu_keyboard(),
        )
        context.user_data["action"] = "add_username"

    elif text == "📥 Bulk Add":
        await update.effective_message.reply_text(
            "Send me the usernames you want to monitor \u2014 one per line, or "
            "comma-separated (e.g., nasa, spacex, vercel)",
            reply_markup=main_menu_keyboard(),
        )
        context.user_data["action"] = "bulk_add_usernames"

    elif text == "🔐 Verify Add":
        await update.effective_message.reply_text(
            "Send me the Instagram username to enable verification monitoring for.\n\n"
            "If it's already on your WatchList, this just turns verification alerts "
            "on for it. If not, it gets added fresh with both ban/unban AND "
            "verification monitoring.",
            reply_markup=main_menu_keyboard(),
        )
        context.user_data["action"] = "verify_add_username"

    elif text == "📋 WatchList":
        await show_user_list(update, user_id, context)

    elif text == "🔍 Check":
        await update.effective_message.reply_text(
            "Send me the Instagram username to check (e.g., nasa)",
            reply_markup=main_menu_keyboard(),
        )
        context.user_data["action"] = "check_status"

    elif text == "🎭 Fake Alert":
        await update.effective_message.reply_text(
            "Which alert do you want to preview?",
            reply_markup=fake_alert_menu_keyboard(),
        )

    elif text == "🗑️ Clear All":
        all_data = await load_data()
        user_key = get_user_key(user_id)
        if user_key not in all_data or not all_data[user_key]:
            await update.effective_message.reply_text(
                "You have no accounts to clear.", reply_markup=main_menu_keyboard()
            )
        else:
            await update.effective_message.reply_text(
                "⚠️ This will remove ALL accounts from monitoring. Sure?",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Yes", callback_data="clear_confirm_yes"),
                        InlineKeyboardButton("❌ No", callback_data="clear_confirm_no"),
                    ]
                ]),
            )

    else:
        action = context.user_data.get("action")
        if action == "add_username":
            await perform_add(update, user_id, text, context)
            del context.user_data["action"]
        elif action == "bulk_add_usernames":
            await perform_bulk_add(update, user_id, text, context)
            del context.user_data["action"]
        elif action == "verify_add_username":
            await perform_verify_add(update, user_id, text, context)
            del context.user_data["action"]
        elif action == "check_status":
            await perform_status_check(update, user_id, text, context)
            del context.user_data["action"]
        elif action == "fake_alert_username":
            fake_event = context.user_data.pop("fake_event", "ban")
            await perform_fake_alert(update, user_id, text, fake_event, context)
            del context.user_data["action"]
        else:
            await update.effective_message.reply_text(
                "Tap a button to get started.", reply_markup=main_menu_keyboard()
            )

# ============================================================================
# BUTTON CALLBACKS
# ============================================================================
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = query.from_user.id

    if not is_authorized(user_id):
        await query.answer("⛔ Not authorized.", show_alert=True)
        return

    await query.answer()
    data = query.data

    if data == "clear_confirm_yes":
        all_data = await load_data()
        user_key = get_user_key(user_id)
        if user_key in all_data:
            del all_data[user_key]
            await save_data(all_data)
        await query.edit_message_text("✅ Cleared all accounts. Your list is now empty.")

    elif data == "clear_confirm_no":
        await query.edit_message_text("👍 Kept all accounts.")

    elif data.startswith("remove_yes:"):
        username_key = data.split(":", 1)[1]
        all_data = await load_data()
        user_key = get_user_key(user_id)
        if user_key in all_data and username_key in all_data[user_key]:
            removed_username = all_data[user_key][username_key]["username"]
            del all_data[user_key][username_key]
            await save_data(all_data)
            await query.edit_message_text(f"🗑️ Removed @{removed_username}.")
            logger.info(f"User {user_id} removed @{removed_username}")

    elif data.startswith("remove_no:"):
        await query.edit_message_text("👍 Kept this account.")

    elif data.startswith("fake:"):
        event = data.split(":", 1)[1]
        context.user_data["fake_event"] = event
        context.user_data["action"] = "fake_alert_username"
        if event == "ban":
            prompt = "Send me the Instagram username to preview a ban alert for."
        else:
            prompt = "Send me a real Instagram username — I'll pull its current stats/avatar for the preview."
        await query.message.reply_text(prompt, reply_markup=main_menu_keyboard())

# ============================================================================
# ACTIONS
# ============================================================================
async def show_user_list(update: Update, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_data = await load_data()
    user_key = get_user_key(user_id)

    if user_key not in all_data or not all_data[user_key]:
        await update.effective_message.reply_text(
            "📭 You're not monitoring any accounts yet.", reply_markup=main_menu_keyboard()
        )
        return

    await update.effective_message.reply_text("📋 <b>WatchList</b>", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

    for account_key, entry in sorted(all_data[user_key].items(), key=lambda kv: kv[1]["username"].lower()):
        link = format_username_link(entry["username"])
        status_line = "🔴 Suspended" if entry["status"] == "suspended" else "🟢 Active"
        last_checked_text = relative_time(entry.get("last_checked", ""))
        verify_line = ""
        if entry.get("is_verified"):
            verify_line += f"\n{EMOJI['verified']} Verified"
        verify_line += f"\n🔐 Verification Monitoring: {'On' if entry.get('verify_watch') else 'Off'}"
        text = (
            f"👤 {link}\n"
            f"{status_line}\n"
            f"🕒 Last Checked: {last_checked_text}{verify_line}"
        )
        await context.application.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=list_item_keyboard(account_key),
        )

async def perform_add(update: Update, user_id: int, username: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    username = username.lstrip("@").strip()
    key = username.lower()

    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await update.effective_message.reply_text(
            "⚠️ Invalid Instagram username format.", reply_markup=main_menu_keyboard()
        )
        return

    all_data = await load_data()
    user_key = get_user_key(user_id)
    if user_key not in all_data:
        all_data[user_key] = {}

    if key in all_data[user_key]:
        await update.effective_message.reply_text(
            f"ℹ️ @{all_data[user_key][key]['username']} is already monitored.",
            reply_markup=main_menu_keyboard(),
        )
        return

    msg = await update.effective_message.reply_text(f"⌛ Checking @{username}")

    result = await check_instagram_status(username)

    now = datetime.now(timezone.utc).isoformat()
    all_data[user_key][key] = {
        "username": username,
        "status": result.status,
        "pending_status": None,
        "pending_count": 0,
        "case_index": 0,
        "last_checked": now,
        "added_at": now,
        "is_verified": result.is_verified,
        "verify_watch": False,
    }
    await save_data(all_data)

    text = build_added_text(username, result)
    profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
    card_path = await asyncio.to_thread(generate_stat_card, username, result.status, result.followers,
                                         result.following, result.posts, profile_pic_bytes, result.is_verified)

    await send_card_with_caption(user_id, context.application, card_path, text, main_menu_keyboard())
    await msg.delete()
    logger.info(f"User {user_id} added @{username} (status: {result.status})")

async def perform_verify_add(update: Update, user_id: int, username: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Turns ON verification monitoring for an account — separate from
    perform_add so a plain ban/unban add never silently starts tracking
    verification. Works whether the account is already monitored (just
    flips the flag) or brand new (adds it with the flag already on).
    Always re-checks first to seed the current verified state as the
    baseline, so turning this on never fires a false initial alert."""
    username = username.lstrip("@").strip()
    key = username.lower()

    if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
        await update.effective_message.reply_text(
            "⚠️ Invalid Instagram username format.", reply_markup=main_menu_keyboard()
        )
        return

    all_data = await load_data()
    user_key = get_user_key(user_id)
    if user_key not in all_data:
        all_data[user_key] = {}

    existing = all_data[user_key].get(key)
    if existing and existing.get("verify_watch"):
        await update.effective_message.reply_text(
            f"ℹ️ Verification monitoring is already ON for @{existing['username']}.",
            reply_markup=main_menu_keyboard(),
        )
        return

    msg = await update.effective_message.reply_text(f"⌛ Checking @{username}")
    case_index = existing.get("case_index", 0) if existing else 0
    result = await check_instagram_status(generate_case_variant(username, case_index))

    if existing:
        existing["is_verified"] = result.is_verified
        existing["verify_watch"] = True
        all_data[user_key][key] = existing
        title = f"🔐 <b>Verification monitoring enabled for @{username}</b>"
    else:
        now = datetime.now(timezone.utc).isoformat()
        all_data[user_key][key] = {
            "username": username, "status": result.status, "pending_status": None,
            "pending_count": 0, "case_index": 0, "last_checked": now, "added_at": now,
            "is_verified": result.is_verified, "verify_watch": True,
        }
        title = f"🔐 <b>Added @{username} with verification monitoring</b>"

    await save_data(all_data)

    state_str = f"verified {EMOJI['verified']}" if result.is_verified else "not verified"
    text = f"{title}\n\nCurrently {state_str}.\n\n<i>{BOT_FOOTER_TEXT}</i>"
    profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
    card_path = await asyncio.to_thread(generate_stat_card, username, result.status, result.followers,
                                         result.following, result.posts, profile_pic_bytes, result.is_verified)

    await send_card_with_caption(user_id, context.application, card_path, text, main_menu_keyboard())
    await msg.delete()
    logger.info(f"User {user_id} enabled verification monitoring for @{username}")

async def perform_bulk_add(update: Update, user_id: int, raw_text: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Parse a block of pasted usernames (one per line, or comma/space separated),
    add each one that isn't already monitored, and reply with a single summary
    message (no per-account stat card images - that would flood the chat)."""
    raw_tokens = re.split(r"[,\n]+", raw_text)
    tokens = []
    for chunk in raw_tokens:
        tokens.extend(chunk.split())

    seen = set()
    usernames = []
    for tok in tokens:
        name = tok.lstrip("@").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        usernames.append(name)

    if not usernames:
        await update.effective_message.reply_text(
            "⚠️ No usernames found in that input.", reply_markup=main_menu_keyboard()
        )
        return

    truncated = False
    if len(usernames) > MAX_BULK_ADD:
        truncated = True
        usernames = usernames[:MAX_BULK_ADD]

    msg = await update.effective_message.reply_text(f"⌛ Checking {len(usernames)} account(s)...")

    all_data = await load_data()
    user_key = get_user_key(user_id)
    if user_key not in all_data:
        all_data[user_key] = {}

    added, skipped_existing, invalid, failed = [], [], [], []

    for i, username in enumerate(usernames):
        key = username.lower()

        if not re.fullmatch(r"[A-Za-z0-9._]{1,30}", username):
            invalid.append(username)
            continue

        if key in all_data[user_key]:
            skipped_existing.append(all_data[user_key][key]["username"])
            continue

        if i > 0:
            delay = INTER_CHECK_DELAY_MS / 1000 + random.uniform(0, 1)
            await asyncio.sleep(delay)

        try:
            result = await check_instagram_status(username)
        except Exception as e:
            logger.exception(f"Bulk add: error checking @{username}: {e}")
            failed.append(username)
            continue

        now = datetime.now(timezone.utc).isoformat()
        all_data[user_key][key] = {
            "username": username,
            "status": result.status,
            "pending_status": None,
            "pending_count": 0,
            "case_index": 0,
            "last_checked": now,
            "added_at": now,
            "is_verified": result.is_verified,
            "verify_watch": False,
        }
        status_icon = "🟢" if result.status == "active" else "🔴"
        vbadge = f" {EMOJI['verified']}" if result.is_verified else ""
        added.append(f"{status_icon} {format_username_link(username)}{vbadge}")

    if added:
        await save_data(all_data)

    lines = ["📥 <b>Bulk Add Results</b>"]
    if added:
        lines.append(f"\n✅ <b>Added ({len(added)})</b>\n" + "\n".join(added))
    if skipped_existing:
        lines.append(
            f"\nℹ️ <b>Already Monitored ({len(skipped_existing)})</b>\n"
            + "\n".join(format_username_link(u) for u in skipped_existing)
        )
    if invalid:
        lines.append(
            f"\n⚠️ <b>Invalid Format ({len(invalid)})</b>\n" + "\n".join(f"@{u}" for u in invalid)
        )
    if failed:
        lines.append(
            f"\n❌ <b>Failed to Check ({len(failed)})</b>\n"
            + "\n".join(format_username_link(u) for u in failed)
        )
    if truncated:
        lines.append(f"\n<i>Only the first {MAX_BULK_ADD} usernames were processed from your input.</i>")
    lines.append(f"\n<i>{BOT_FOOTER_TEXT}</i>")

    await update.effective_message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard()
    )
    await msg.delete()
    logger.info(f"User {user_id} bulk-added {len(added)} account(s)")

async def perform_status_check(update: Update, user_id: int, username: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    username = username.lstrip("@").strip()
    msg = await update.effective_message.reply_text(f"⌛ Checking @{username}")

    result = await check_instagram_status(username)

    text = build_check_text(username, result)

    profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
    card_path = await asyncio.to_thread(generate_stat_card, username, result.status, result.followers,
                                         result.following, result.posts, profile_pic_bytes, result.is_verified)

    await send_card_with_caption(user_id, context.application, card_path, text, main_menu_keyboard())
    await msg.delete()

async def perform_fake_alert(update: Update, user_id: int, username: str, event: str,
                              context: ContextTypes.DEFAULT_TYPE) -> None:
    """event: 'ban' | 'unban' | 'verify_on' | 'verify_off'. Mirrors the
    Discord Advanced bot's /fake exactly: pulls the real account's current
    stats/avatar for unban/verify_on/verify_off (nothing real to pull for
    a ban, so that one stays a synthetic "not found" card), and renders
    with the same build_event_text/generate_stat_card used for a real
    alert — no watermark, no "this is a preview" text, identical output."""
    username = username.lstrip("@").strip()
    msg = await update.effective_message.reply_text(f"⌛ Building preview for @{username}")
    status_label, ring_color = EVENT_CARD_STYLE[event]

    if event == "ban":
        text = build_event_text("ban", username, time_taken_str="3h 2m")
        card_path = await asyncio.to_thread(generate_stat_card, username, "suspended",
                                             status_label=status_label, ring_color=ring_color)
    else:
        result = await check_instagram_status(username)
        profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
        real_followers = result.followers if result.followers is not None else 19614
        # The verify badge reflects the event being previewed (that's the
        # whole point of "gained"/"expired"), not the account's live status.
        if event == "verify_on":
            is_verified = True
        elif event == "verify_off":
            is_verified = False
        else:
            is_verified = result.is_verified

        text = build_event_text(event, username, real_followers, "3h 2m", is_verified)
        card_path = await asyncio.to_thread(generate_stat_card, username, "active", real_followers,
                                             result.following, result.posts, profile_pic_bytes, is_verified,
                                             status_label=status_label, ring_color=ring_color)

    await send_card_with_caption(user_id, context.application, card_path, text, main_menu_keyboard())
    await msg.delete()

# ============================================================================
# KEEP-ALIVE SERVER (unchanged)
# ============================================================================
async def healthz(request: web.Request) -> web.Response:
    return web.Response(text="ok")

async def start_keepalive_server() -> None:
    app = web.Application()
    app.router.add_get("/", healthz)
    app.router.add_get("/healthz", healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logger.info(f"Keep-alive server listening on 0.0.0.0:{PORT} (/healthz)")

# ============================================================================
# POLLING LOOP (ban/unban both directions + opt-in verification monitoring)
# ============================================================================
async def notify_user(app: Application, user_id: int, text: str, card_path: Optional[str] = None,
                       reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        if card_path:
            await send_card_with_caption(user_id, app, card_path, text, reply_markup)
        else:
            await app.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    except Forbidden:
        logger.warning(f"Bot blocked by user {user_id}")
    except (BadRequest, TelegramError) as e:
        logger.error(f"Error notifying user {user_id}: {e}")

async def poll_once(app: Application) -> None:
    all_data = await load_data()
    if not all_data:
        return

    for user_key, user_accounts in list(all_data.items()):
        user_id = int(user_key.split("_")[1])

        for account_key, entry in list(user_accounts.items()):
            original_username = entry["username"]

            current_case_index = entry.get("case_index", 0)
            username = generate_case_variant(original_username, current_case_index)
            entry["case_index"] = next_case_index(original_username, current_case_index)

            delay = INTER_CHECK_DELAY_MS / 1000 + random.uniform(0, 1)
            await asyncio.sleep(delay)

            try:
                result = await check_instagram_status(username)
            except Exception as e:
                logger.exception(f"Error checking @{original_username}: {e}")
                continue

            entry["last_checked"] = datetime.now(timezone.utc).isoformat()

            if result.status != entry["status"]:
                if entry.get("pending_status") == result.status:
                    entry["pending_count"] = entry.get("pending_count", 0) + 1
                else:
                    entry["pending_status"] = result.status
                    entry["pending_count"] = 1

                if entry["pending_count"] >= CONFIRMATION_THRESHOLD:
                    old_status = entry["status"]
                    entry["status"] = result.status
                    entry["pending_status"] = None
                    entry["pending_count"] = 0

                    time_taken_str = compute_time_taken(entry)

                    if old_status == "active" and result.status == "suspended":
                        text = build_event_text("ban", original_username, time_taken_str=time_taken_str)
                        status_label, ring_color = EVENT_CARD_STYLE["ban"]
                        card_path = await asyncio.to_thread(generate_stat_card, original_username, "suspended",
                                                             status_label=status_label, ring_color=ring_color)

                    elif old_status == "suspended" and result.status == "active":
                        text = build_event_text("unban", original_username, result.followers, time_taken_str, result.is_verified)
                        profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
                        status_label, ring_color = EVENT_CARD_STYLE["unban"]
                        card_path = await asyncio.to_thread(generate_stat_card, original_username, "active", result.followers,
                                                             result.following, result.posts, profile_pic_bytes, result.is_verified,
                                                             status_label=status_label, ring_color=ring_color)

                    else:
                        text = f"Status changed: {old_status} → {result.status}\n\n{format_username_link(original_username)}"
                        card_path = None

                    keyboard = removal_keyboard(account_key)
                    await notify_user(app, user_id, text, card_path, keyboard)
                    logger.info(f"User {user_id}: @{original_username} {old_status} → {result.status}")
            else:
                entry["pending_status"] = None
                entry["pending_count"] = 0

            # Verification diffing — opt-in per account via verify_watch.
            # is_verified is always tracked silently (for WatchList display)
            # even when verify_watch is off; only the ALERT is gated.
            if result.status == "active":
                old_verified = bool(entry.get("is_verified", False))
                new_verified = bool(result.is_verified)
                verify_watch = bool(entry.get("verify_watch", False))
                if new_verified != old_verified:
                    entry["is_verified"] = new_verified
                    if verify_watch:
                        v_time_taken_str = compute_time_taken(entry)
                        vevent = "verify_on" if new_verified else "verify_off"
                        v_text = build_event_text(vevent, original_username, result.followers, v_time_taken_str, new_verified)
                        v_profile_pic_bytes = await fetch_profile_pic_bytes(result.profile_pic_url)
                        v_status_label, v_ring_color = EVENT_CARD_STYLE[vevent]
                        v_card_path = await asyncio.to_thread(generate_stat_card, original_username, "active", result.followers,
                                                               result.following, result.posts, v_profile_pic_bytes, new_verified,
                                                               status_label=v_status_label, ring_color=v_ring_color)
                        await notify_user(app, user_id, v_text, v_card_path, removal_keyboard(account_key))
                        logger.info(f"User {user_id}: @{original_username} verification -> {new_verified}")
                elif "is_verified" not in entry:
                    entry["is_verified"] = new_verified

            user_accounts[account_key] = entry

        all_data[user_key] = user_accounts

    await save_data(all_data)

async def polling_loop(app: Application) -> None:
    logger.info(f"Starting polling loop (interval={POLL_INTERVAL_SECONDS}s)")
    await asyncio.sleep(2)
    while True:
        try:
            await poll_once(app)
        except Exception as e:
            logger.exception(f"Polling error: {e}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

# ============================================================================
# LIFECYCLE
# ============================================================================
async def on_startup(app: Application) -> None:
    logger.info("Starting up...")
    await start_keepalive_server()
    if PROXY_AUTO_FETCH:
        # Don't block bot startup on this — a free-proxy list can have
        # hundreds of dead candidates and take well over a minute to
        # health-check. The pool just starts empty (falling back to no
        # proxy) until the first background refresh completes.
        app.create_task(proxy_refresh_loop(), update=True)
    app.create_task(polling_loop(app), update=True)
    logger.info("Bot startup complete.")

async def on_shutdown(app: Application) -> None:
    logger.info("Bot shutdown complete.")

# ============================================================================
# MAIN
# ============================================================================
def main() -> None:
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("Starting Telegram bot polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
