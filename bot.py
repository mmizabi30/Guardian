
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Guardian Telegram Bot (Web Service + Polling)

ویژگی‌ها:
- فقط در یک گروهِ تعیین‌شده فعال می‌شود
- پیام‌های عکس / ویدیو / گیف / استیکر / ویدیو نوت را حذف می‌کند
- فقط ویس را اجازه می‌دهد
- هر لینک سایت/کانال/گروه حذف می‌شود مگر:
    • فرستنده «ادمین اصلی» (OWNER) باشد
    • لینک، لینک استارت خود همین ربات یا یکی از «ربات‌های مجاز» ثبت‌شده در پنل باشد
- فیلتر کلمات دارد
- کاربر ویژه با «تنظیم ویژه» / «حذف ویژه» (روی پیام کاربر با ریپلای)
- کاربران ویژه و ادمین‌ها می‌توانند با فرستادن «تگ» همه‌ی اعضا را (به‌صورت فشرده در قالب یک کلمه‌ی کوچک) منشن کنند
- ادمین اصلی + ادمین‌های اضافه‌شده از پنل خصوصی
- ذخیره همه تنظیمات در Neon/PostgreSQL
- بدون webhook اجرا می‌شود، ولی برای Render Web Service یک HTTP health endpoint دارد

اجرا:
  pip install -r requirements_web.txt
  export BOT_TOKEN="123456:ABC..."
  export DATABASE_URL="postgresql://..."
  python guardian_bot_web_ready.py

نکته:
  بات باید در گروه هدف ادمین باشد و اجازه حذف پیام داشته باشد.
"""

import asyncio
import json
import logging
import os
import re
from html import escape as html_escape
from typing import Any, Optional

import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ContentType, MessageEntityType
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =====================
# تنظیمات اصلی
# =====================

OWNER_ID = int(os.getenv("OWNER_ID", "8883527571"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PORT = int(os.getenv("PORT", "10000"))

# اگر خود بات بالا آمد، یوزرنیمش از getMe خوانده می‌شود؛ این فقط fallback است
ALLOWED_BOT_USERNAME = os.getenv("ALLOWED_BOT_USERNAME", "").strip()

MENTION_CHUNK_LIMIT = 4050  # سقف واقعی پیام تلگرام ۴۰۹۶ کاراکتره؛ این عدد با کمی حاشیه‌ی امن نزدیک به همونه
TAG_SENTENCE_WORDS = ["تمامی", "کاربر", "ها", "درحال", "تگ", "شن👀"]  # جمله‌ی نمایشی تگ
TAG_SEPARATOR = "\u200c"  # نیم‌فاصله (ZWNJ) - همون کاراکتر جداکننده‌ی کیبورد فارسی

# =====================
# لاگ
# =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("guardian-bot")

# =====================
# متغیرهای runtime
# =====================

db_pool: asyncpg.Pool | None = None
runtime_bot_username: str = ""

router = Router()


# =====================
# دیتابیس
# =====================

async def db_exec(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        return await conn.execute(query, *args)


async def db_fetch(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def db_fetchrow(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def init_db() -> None:
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS filters (
                word TEXT PRIMARY KEY
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS special_users (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                added_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                added_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_members (
                user_id BIGINT PRIMARY KEY,
                first_name TEXT,
                username TEXT,
                full_name TEXT,
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_state (
                admin_id BIGINT PRIMARY KEY,
                state TEXT,
                payload JSONB
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS allowed_bots (
                username TEXT PRIMARY KEY,
                added_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )


async def db_get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    row = await db_fetchrow("SELECT value FROM settings WHERE key = $1", key)
    return row["value"] if row else default


async def db_set_setting(key: str, value: str) -> None:
    await db_exec(
        """
        INSERT INTO settings(key, value) VALUES($1, $2)
        ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
        """,
        key,
        value,
    )


async def db_del_setting(key: str) -> None:
    await db_exec("DELETE FROM settings WHERE key = $1", key)


async def db_list_filters() -> list[str]:
    rows = await db_fetch("SELECT word FROM filters ORDER BY word ASC")
    return [r["word"] for r in rows]


async def db_add_filter(word: str) -> bool:
    word = normalize_filter(word)
    if not word:
        return False
    try:
        await db_exec("INSERT INTO filters(word) VALUES($1)", word)
        return True
    except asyncpg.UniqueViolationError:
        return False


async def db_remove_filter(word: str) -> bool:
    word = normalize_filter(word)
    result = await db_exec("DELETE FROM filters WHERE word = $1", word)
    return result.endswith(" 1")


async def db_list_special_users() -> list[asyncpg.Record]:
    return await db_fetch(
        "SELECT user_id, first_name, username, added_at FROM special_users ORDER BY added_at DESC"
    )


async def db_add_special_user(user_id: int, first_name: str | None, username: str | None) -> None:
    await db_exec(
        """
        INSERT INTO special_users(user_id, first_name, username)
        VALUES($1, $2, $3)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            username = EXCLUDED.username
        """,
        user_id,
        first_name,
        username,
    )


async def db_remove_special_user(user_id: int) -> bool:
    result = await db_exec("DELETE FROM special_users WHERE user_id = $1", user_id)
    return result.endswith(" 1")


async def db_is_special_user(user_id: int) -> bool:
    row = await db_fetchrow("SELECT 1 FROM special_users WHERE user_id = $1 LIMIT 1", user_id)
    return row is not None


async def db_add_admin(user_id: int, first_name: str | None, username: str | None) -> None:
    if user_id == OWNER_ID:
        return
    await db_exec(
        """
        INSERT INTO admins(user_id, first_name, username)
        VALUES($1, $2, $3)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            username = EXCLUDED.username
        """,
        user_id,
        first_name,
        username,
    )


async def db_remove_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return False
    result = await db_exec("DELETE FROM admins WHERE user_id = $1", user_id)
    return result.endswith(" 1")


async def db_list_admins() -> list[dict[str, Any]]:
    rows = await db_fetch("SELECT user_id, first_name, username, added_at FROM admins ORDER BY added_at DESC")
    admins = [
        {
            "user_id": OWNER_ID,
            "first_name": "OWNER",
            "username": None,
            "added_at": None,
            "owner": True,
        }
    ]
    for row in rows:
        admins.append(
            {
                "user_id": int(row["user_id"]),
                "first_name": row["first_name"],
                "username": row["username"],
                "added_at": row["added_at"],
                "owner": False,
            }
        )
    return admins


async def db_is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    row = await db_fetchrow("SELECT 1 FROM admins WHERE user_id = $1 LIMIT 1", user_id)
    return row is not None


async def db_set_admin_state(admin_id: int, state: Optional[str], payload: Optional[dict[str, Any]] = None) -> None:
    if state is None:
        await db_exec("DELETE FROM admin_state WHERE admin_id = $1", admin_id)
        return

    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    await db_exec(
        """
        INSERT INTO admin_state(admin_id, state, payload) VALUES($1, $2, $3::jsonb)
        ON CONFLICT(admin_id) DO UPDATE SET
            state = EXCLUDED.state,
            payload = EXCLUDED.payload
        """,
        admin_id,
        state,
        payload_json,
    )


async def db_get_admin_state(admin_id: int) -> tuple[Optional[str], dict[str, Any]]:
    row = await db_fetchrow(
        "SELECT state, payload FROM admin_state WHERE admin_id = $1",
        admin_id,
    )
    if not row:
        return None, {}
    payload: dict[str, Any] = {}
    if row["payload"]:
        try:
            payload = dict(row["payload"])
        except Exception:
            payload = {}
    return row["state"], payload


async def db_add_group_member(user_id: int, first_name: str | None, username: str | None, full_name: str | None) -> None:
    await db_exec(
        """
        INSERT INTO group_members(user_id, first_name, username, full_name, last_seen)
        VALUES($1, $2, $3, $4, NOW())
        ON CONFLICT(user_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            username = EXCLUDED.username,
            full_name = EXCLUDED.full_name,
            last_seen = NOW()
        """,
        user_id,
        first_name,
        username,
        full_name,
    )


async def db_list_group_members() -> list[asyncpg.Record]:
    return await db_fetch(
        "SELECT user_id, first_name, username, full_name, last_seen FROM group_members ORDER BY last_seen DESC"
    )


async def db_add_allowed_bot(username: str) -> bool:
    username = username.lower()
    try:
        await db_exec("INSERT INTO allowed_bots(username) VALUES($1)", username)
        return True
    except asyncpg.UniqueViolationError:
        return False


async def db_remove_allowed_bot(username: str) -> bool:
    result = await db_exec("DELETE FROM allowed_bots WHERE username = $1", username.lower())
    return result.endswith(" 1")


async def db_list_allowed_bots() -> list[str]:
    rows = await db_fetch("SELECT username FROM allowed_bots ORDER BY added_at DESC")
    return [r["username"] for r in rows]


# =====================
# ابزارها
# =====================

def normalize_filter(word: str) -> str:
    return re.sub(r"\s+", " ", (word or "").strip().casefold())


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().casefold())


def normalize_action_text(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"^[\s\(\[\{«\"'']+|[\s\)\]\}»\"'']+$", "", t)
    return re.sub(r"\s+", " ", t).casefold()


def safe_int(text: str) -> Optional[int]:
    try:
        return int(str(text).strip())
    except Exception:
        return None


def normalize_bot_username(text: str) -> Optional[str]:
    """
    ورودی می‌تواند یوزرنیم خام (@somebot / somebot) یا لینک t.me/somebot باشد.
    """
    t = (text or "").strip()
    if not t:
        return None

    m = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,32})", t, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    t = t.lstrip("@").strip()
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", t):
        return t.lower()

    return None


def is_rules_trigger(text: str) -> bool:
    return normalize_action_text(text) == "قوانین"


def is_tag_command(text: str) -> bool:
    return normalize_action_text(text) == "تگ"


def is_special_add_command(text: str) -> bool:
    return normalize_action_text(text) == "تنظیم ویژه"


def is_special_remove_command(text: str) -> bool:
    return normalize_action_text(text) == "حذف ویژه"


def iter_message_entities(message: Message):
    text = message.text or ""
    caption = message.caption or ""

    if message.entities:
        for ent in message.entities:
            yield ("text", text, ent)
    if message.caption_entities:
        for ent in message.caption_entities:
            yield ("caption", caption, ent)


def entity_url(text: str, ent) -> Optional[str]:
    if ent.type == MessageEntityType.URL:
        return text[ent.offset : ent.offset + ent.length]
    if ent.type == MessageEntityType.TEXT_LINK:
        return getattr(ent, "url", None)
    return None


def bot_username_for_links() -> str:
    return runtime_bot_username or ALLOWED_BOT_USERNAME or ""


async def is_allowed_bot_start_link(url: str) -> bool:
    """
    فقط لینک‌های استارت (t.me/username?start=...) خود ربات یا ربات‌های
    ثبت‌شده در «ربات‌های مجاز» (پنل ادمین) مجاز هستند.
    """
    if not url:
        return False

    url_clean = url.strip().rstrip(".,;!؟?)]}»\"'")
    match = re.fullmatch(
        r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,32})\?start=[A-Za-z0-9_\-]+",
        url_clean,
        re.IGNORECASE,
    )
    if not match:
        return False

    username = match.group(1).lower()

    own = bot_username_for_links().lower()
    if own and username == own:
        return True

    allowed = await db_list_allowed_bots()
    return username in {u.lower() for u in allowed if u}


async def has_disallowed_link(message: Message) -> bool:
    for _, text, ent in iter_message_entities(message):
        if ent.type not in {MessageEntityType.URL, MessageEntityType.TEXT_LINK}:
            continue
        url = entity_url(text, ent)
        if not url:
            continue
        if await is_allowed_bot_start_link(url):
            continue
        return True
    return False


def has_username_mention(message: Message) -> bool:
    text = f"{message.text or ''} {message.caption or ''}"

    # visible @username
    if re.search(r"(?<![A-Za-z0-9_])@[A-Za-z0-9_]{5,32}\b", text):
        return True

    # Telegram parsed mentions by username
    for _, _, ent in iter_message_entities(message):
        if ent.type == MessageEntityType.MENTION:
            return True

    return False


def has_explicit_user_id(message: Message) -> bool:
    text = normalize_text(f"{message.text or ''} {message.caption or ''}")

    if "tg://user?id=" in text:
        return False

    # الگوهای رایج برای ارسال آیدی
    patterns = [
        r"\b(?:id|user id|آیدی|ایدی)\b\s*[:：\-]?\s*\d{5,}",
        r"\b(?:tg id|telegram id)\b\s*[:：]?\s*\d{5,}",
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def has_disallowed_user_identifier(message: Message) -> bool:
    return has_username_mention(message) or has_explicit_user_id(message)


async def has_filter_match(message: Message) -> bool:
    filters = await db_list_filters()
    if not filters:
        return False

    haystack = normalize_text((message.text or "") + " " + (message.caption or ""))
    if not haystack:
        return False

    return any(f in haystack for f in filters)


def is_blocked_media(message: Message, special: bool) -> bool:
    ctype = message.content_type

    if ctype in {ContentType.PHOTO, ContentType.VIDEO}:
        return True

    if ctype in {ContentType.STICKER, ContentType.ANIMATION, ContentType.VIDEO_NOTE}:
        return not special

    return False


def build_tag_messages(user_ids: list[int], max_chars: int = MENTION_CHUNK_LIMIT) -> list[str]:
    """
    برای هر پیام تگ:
    - اول کلمات جمله‌ی «تمامی کاربر ها درحال تگ شن👀» هر کدام به یک کاربر منشن می‌شوند
      (همون کلمات با فاصله‌ی عادی بین‌شون، پیام کاملاً طبیعی و خوانا دیده می‌شه).
    - بعد، با کاراکتر نیم‌فاصله (ZWNJ) که نامرئیه، بقیه‌ی کاربران هم به همون پیام
      اضافه می‌شن (هر نیم‌فاصله = منشن یک کاربر) تا سقف مجاز پیام پر بشه.
    - وقتی یک پیام پر شد، بقیه‌ی کاربران می‌رن توی پیام تگ بعدی (دوباره با همون جمله).
    این‌طوری تلگرام پیام رو رد نمی‌کنه (چون فقط از کاراکتر جداکننده تشکیل نشده)
    و ظاهر پیام هم کاملاً کوتاه و طبیعی می‌مونه.
    """
    messages: list[str] = []
    idx = 0
    total = len(user_ids)

    while idx < total:
        word_tokens: list[str] = []
        for word in TAG_SENTENCE_WORDS:
            if idx < total:
                uid = user_ids[idx]
                idx += 1
                word_tokens.append(f'<a href="tg://user?id={uid}">{word}</a>')
            else:
                word_tokens.append(word)

        body = " ".join(word_tokens)
        current_len = len(body)

        filler_parts: list[str] = []
        while idx < total:
            uid = user_ids[idx]
            token = f'<a href="tg://user?id={uid}">{TAG_SEPARATOR}</a>'
            token_len = len(token)
            if current_len + token_len > max_chars:
                break
            filler_parts.append(token)
            current_len += token_len
            idx += 1

        if filler_parts:
            body += "".join(filler_parts)

        messages.append(body)

    return messages


async def delete_message_safe(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        log.warning("delete_message failed chat=%s msg=%s err=%s", chat_id, message_id, e)


def kb_main_menu(is_owner: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="تنظیم گروه", callback_data="menu:set_group")
    kb.button(text="تنظیمات فیلتر", callback_data="menu:filters")
    kb.button(text="متن قوانین", callback_data="menu:rules")
    kb.button(text="لیست کاربران ویژه", callback_data="menu:specials")
    kb.button(text="ربات‌های مجاز", callback_data="menu:allowed_bots")
    if is_owner:
        kb.button(text="مدیریت ادمین‌ها", callback_data="menu:admins")
    kb.button(text="گروه فعال", callback_data="menu:show_group")
    kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()


def kb_allowed_bots_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="افزودن ربات مجاز", callback_data="allowedbot:add")
    kb.button(text="لیست ربات‌های مجاز", callback_data="allowedbot:list")
    kb.button(text="حذف ربات مجاز", callback_data="allowedbot:remove")
    kb.button(text="بازگشت", callback_data="menu:home")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()


def kb_filter_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="تنظیم فیلتر جدید", callback_data="filter:add")
    kb.button(text="لیست فیلترها", callback_data="filter:list")
    kb.button(text="حذف فیلتر", callback_data="filter:remove")
    kb.button(text="بازگشت", callback_data="menu:home")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()


def kb_special_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="راهنمای ویژه", callback_data="special:help")
    kb.button(text="لیست ویژه", callback_data="special:list")
    kb.button(text="بازگشت", callback_data="menu:home")
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def kb_admin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="افزودن ادمین", callback_data="admin:add")
    kb.button(text="لیست ادمین‌ها", callback_data="admin:list")
    kb.button(text="حذف ادمین", callback_data="admin:remove")
    kb.button(text="بازگشت", callback_data="menu:home")
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()


def kb_back_home() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="بازگشت به منو", callback_data="menu:home")
    return kb.as_markup()


async def fmt_group_text() -> str:
    group_id = await db_get_setting("target_chat_id")
    group_title = await db_get_setting("target_chat_title")

    if not group_id:
        return "هنوز هیچ گروهی تنظیم نشده است."

    if group_title:
        return f"گروه فعال:\n{group_title}\nID: {group_id}"
    return f"گروه فعال:\nID: {group_id}"


async def fmt_filters_text() -> str:
    items = await db_list_filters()
    if not items:
        return "فعلاً هیچ فیلتر کلمه‌ای ثبت نشده است."
    return "لیست فیلترها:\n" + "\n".join(f"• {w}" for w in items)


async def fmt_specials_text() -> str:
    rows = await db_list_special_users()
    if not rows:
        return "فعلاً هیچ کاربر ویژه‌ای ثبت نشده است."
    lines = []
    for r in rows:
        name = r["first_name"] or "بدون نام"
        username = f"@{r['username']}" if r["username"] else "-"
        lines.append(f"• {html_escape(name)} | {html_escape(username)} | {r['user_id']}")
    return "کاربران ویژه:\n" + "\n".join(lines)


async def fmt_admins_text() -> str:
    admins = await db_list_admins()
    lines = []
    for a in admins:
        name = a["first_name"] or "بدون نام"
        username = f"@{a['username']}" if a["username"] else "-"
        role = "مالک اصلی" if a.get("owner") else "ادمین"
        lines.append(f"• {html_escape(name)} | {html_escape(username)} | {a['user_id']} | {role}")
    return "ادمین‌ها:\n" + "\n".join(lines)


async def fmt_allowed_bots_text() -> str:
    items = await db_list_allowed_bots()
    if not items:
        return "فعلاً هیچ ربات مجازی ثبت نشده است."
    return "ربات‌های مجاز (لینک استارت‌شان حذف نمی‌شود):\n" + "\n".join(f"• @{u}" for u in items)


async def get_allowed_target_chat_id() -> Optional[int]:
    val = await db_get_setting("target_chat_id")
    return safe_int(val) if val else None


async def get_rules_text() -> str:
    return await db_get_setting("rules_text", "متن قوانین هنوز تنظیم نشده است.") or "متن قوانین هنوز تنظیم نشده است."


async def get_owner_id(message: Message) -> int:
    return message.from_user.id if message.from_user else 0


async def parse_admin_target_from_message(message: Message) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    از پیام خصوصی ادمین برای افزودن/حذف ادمین استفاده می‌شود.
    """
    user_id: Optional[int] = None
    first_name: Optional[str] = None
    username: Optional[str] = None

    if getattr(message, "forward_from", None):
        fwd = message.forward_from
        user_id = int(fwd.id)
        first_name = fwd.first_name
        username = fwd.username

    elif getattr(message, "forward_origin", None):
        origin = message.forward_origin
        user = getattr(origin, "user", None)
        if user and getattr(user, "id", None) is not None:
            user_id = int(user.id)
            first_name = getattr(user, "first_name", None)
            username = getattr(user, "username", None)

    if user_id is None:
        user_id = safe_int(message.text or "")

    return user_id, first_name, username


# =====================
# پنل خصوصی ادمین
# =====================

async def maybe_handle_admin_private(message: Message, bot: Bot) -> bool:
    if not message.from_user or not await db_is_admin(message.from_user.id):
        return False

    user_id = message.from_user.id
    is_owner = user_id == OWNER_ID
    state, _payload = await db_get_admin_state(user_id)
    text = (message.text or "").strip()

    if text in {"/start", "/panel", "پنل", "منو"} and not state:
        await message.answer(
            "پنل مدیریت بات:\n"
            "از دکمه‌ها برای تنظیم گروه، فیلترها، متن قوانین، کاربران ویژه و ادمین‌ها استفاده کن.",
            reply_markup=kb_main_menu(is_owner),
        )
        return True

    if text == "/cancel":
        await db_set_admin_state(user_id, None)
        await message.answer("عملیات جاری لغو شد.", reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_group_id":
        group_id = safe_int(text)
        if group_id is None and message.forward_from_chat:
            group_id = int(message.forward_from_chat.id)

        if group_id is None and getattr(message, "forward_origin", None):
            origin = message.forward_origin
            chat = getattr(origin, "chat", None)
            if chat and getattr(chat, "id", None) is not None:
                group_id = int(chat.id)

        if group_id is None:
            await message.answer(
                "ID گروه را بفرست یا یک پیام فوروارد شده از همان گروه را ارسال کن.",
                reply_markup=kb_back_home(),
            )
            return True

        group_title = None
        if message.forward_from_chat and message.forward_from_chat.title:
            group_title = message.forward_from_chat.title
        elif getattr(message, "forward_origin", None):
            origin = message.forward_origin
            chat = getattr(origin, "chat", None)
            if chat and getattr(chat, "title", None):
                group_title = chat.title

        await db_set_setting("target_chat_id", str(group_id))
        if group_title:
            await db_set_setting("target_chat_title", group_title)
        else:
            await db_del_setting("target_chat_title")

        await db_set_admin_state(user_id, None)
        await message.answer(
            f"گروه با موفقیت تنظیم شد.\n{await fmt_group_text()}",
            reply_markup=kb_main_menu(is_owner),
        )
        return True

    if state == "await_rules_text":
        rules_text = (message.text or message.caption or "").strip()
        if not rules_text:
            await message.answer("متن قوانین خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        await db_set_setting("rules_text", rules_text)
        await db_set_admin_state(user_id, None)
        await message.answer("متن قوانین ذخیره شد.", reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_filter_add":
        word = normalize_filter(text)
        if not word:
            await message.answer("فیلتر خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        added = await db_add_filter(word)
        await db_set_admin_state(user_id, None)
        msg = f"فیلتر اضافه شد: {word}" if added else f"این فیلتر از قبل وجود دارد: {word}"
        await message.answer(msg, reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_filter_remove":
        word = normalize_filter(text)
        if not word:
            await message.answer("فیلتر خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        removed = await db_remove_filter(word)
        await db_set_admin_state(user_id, None)
        msg = f"فیلتر حذف شد: {word}" if removed else f"این فیلتر پیدا نشد: {word}"
        await message.answer(msg, reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_admin_add":
        target_id, first_name, username = await parse_admin_target_from_message(message)
        if target_id is None:
            await message.answer(
                "ID عددی ادمین را بفرست یا یک پیام/فوروارد از همان کاربر ارسال کن.\n"
                "برای لغو: /cancel",
                reply_markup=kb_back_home(),
            )
            return True

        if target_id == OWNER_ID:
            await db_set_admin_state(user_id, None)
            await message.answer("این کاربر همان ادمین اصلی است.", reply_markup=kb_main_menu(is_owner))
            return True

        await db_add_admin(target_id, first_name, username)
        await db_set_admin_state(user_id, None)
        await message.answer(f"ادمین اضافه شد: {target_id}", reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_admin_remove":
        target_id, _, _ = await parse_admin_target_from_message(message)
        if target_id is None:
            await message.answer(
                "ID عددی ادمین را بفرست.\nبرای لغو: /cancel",
                reply_markup=kb_back_home(),
            )
            return True

        removed = await db_remove_admin(target_id)
        await db_set_admin_state(user_id, None)
        msg = "ادمین حذف شد." if removed else "این ادمین پیدا نشد یا قابل حذف نیست."
        await message.answer(msg, reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_allowed_bot_add":
        username = normalize_bot_username(text)
        if not username:
            await message.answer(
                "یوزرنیم معتبر نیست. دوباره بفرست (مثلاً @somebot یا لینک t.me/somebot).\n"
                "برای لغو: /cancel",
                reply_markup=kb_back_home(),
            )
            return True

        added = await db_add_allowed_bot(username)
        await db_set_admin_state(user_id, None)
        msg = f"ربات مجاز اضافه شد: @{username}" if added else f"این ربات از قبل مجاز است: @{username}"
        await message.answer(msg, reply_markup=kb_main_menu(is_owner))
        return True

    if state == "await_allowed_bot_remove":
        username = normalize_bot_username(text)
        if not username:
            await message.answer(
                "یوزرنیم معتبر نیست. دوباره بفرست.\nبرای لغو: /cancel",
                reply_markup=kb_back_home(),
            )
            return True

        removed = await db_remove_allowed_bot(username)
        await db_set_admin_state(user_id, None)
        msg = f"ربات مجاز حذف شد: @{username}" if removed else f"این ربات در لیست مجازها پیدا نشد: @{username}"
        await message.answer(msg, reply_markup=kb_main_menu(is_owner))
        return True

    if text == "/start":
        await message.answer("برای مدیریت بات /panel را بزن.", reply_markup=kb_main_menu(is_owner))
        return True

    return False


# =====================
# کال‌بک‌ها
# =====================

@router.callback_query()
async def callback_handler(callback: CallbackQuery):
    if not callback.from_user or not await db_is_admin(callback.from_user.id):
        await callback.answer("دسترسی ندارید.", show_alert=False)
        return

    user_id = callback.from_user.id
    is_owner = user_id == OWNER_ID
    data = callback.data or ""
    await callback.answer()

    if data == "menu:home":
        await db_set_admin_state(user_id, None)
        await callback.message.edit_text(
            "پنل مدیریت بات:\n"
            "از دکمه‌ها برای تنظیم گروه، فیلترها، متن قوانین، کاربران ویژه و ادمین‌ها استفاده کن.",
            reply_markup=kb_main_menu(is_owner),
        )
        return

    if data == "menu:show_group":
        await callback.message.edit_text(await fmt_group_text(), reply_markup=kb_main_menu(is_owner))
        return

    if data == "menu:set_group":
        await db_set_admin_state(user_id, "await_group_id")
        await callback.message.edit_text(
            "حالا ID گروه را بفرست یا یک پیام فوروارد شده از همان گروه را ارسال کن.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "menu:rules":
        await db_set_admin_state(user_id, "await_rules_text")
        await callback.message.edit_text(
            "متن کامل قوانین را همین‌جا بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "menu:filters":
        await callback.message.edit_text("تنظیمات فیلترها:", reply_markup=kb_filter_menu())
        return

    if data == "menu:specials":
        await callback.message.edit_text(
            "تنظیم کاربران ویژه:\n"
            "در گروهِ هدف روی پیام کاربر ریپلای کن و «تنظیم ویژه» یا «حذف ویژه» بفرست.\n"
            "کاربر ویژه می‌تواند استیکر، گیف و ویدیو نوت بفرستد.",
            reply_markup=kb_special_menu(),
        )
        return

    if data == "menu:admins":
        if not is_owner:
            await callback.message.edit_text("فقط ادمین اصلی می‌تواند ادمین اضافه/حذف کند.", reply_markup=kb_main_menu(is_owner))
            return
        await callback.message.edit_text("مدیریت ادمین‌ها:", reply_markup=kb_admin_menu())
        return

    if data == "menu:allowed_bots":
        await callback.message.edit_text(
            "مدیریت ربات‌های مجاز:\n"
            "لینک استارت (t.me/username?start=...) ربات‌های زیر حذف نمی‌شود.\n"
            "لینک استارت خود همین ربات همیشه مجاز است.",
            reply_markup=kb_allowed_bots_menu(),
        )
        return

    if data == "allowedbot:add":
        await db_set_admin_state(user_id, "await_allowed_bot_add")
        await callback.message.edit_text(
            "یوزرنیم ربات مورد نظر را بفرست (مثلاً @somebot یا somebot یا لینک t.me/somebot).\n"
            "برای لغو: /cancel"
        )
        return

    if data == "allowedbot:list":
        await callback.message.edit_text(await fmt_allowed_bots_text(), reply_markup=kb_allowed_bots_menu())
        return

    if data == "allowedbot:remove":
        await db_set_admin_state(user_id, "await_allowed_bot_remove")
        await callback.message.edit_text(
            "یوزرنیم رباتی که می‌خواهی از لیست مجاز حذف شود را بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "filter:add":
        await db_set_admin_state(user_id, "await_filter_add")
        await callback.message.edit_text(
            "کلمه یا عبارت فیلتر جدید را بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "filter:list":
        await callback.message.edit_text(await fmt_filters_text(), reply_markup=kb_filter_menu())
        return

    if data == "filter:remove":
        await db_set_admin_state(user_id, "await_filter_remove")
        await callback.message.edit_text(
            "کلمه یا عبارت فیلتر را برای حذف بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "special:help":
        await callback.message.edit_text(
            "روش استفاده:\n"
            "1) در گروه هدف روی پیام کاربر ریپلای کن.\n"
            "2) «تنظیم ویژه» بفرست تا ویژه شود.\n"
            "3) «حذف ویژه» بفرست تا ویژه حذف شود.\n\n"
            "کاربر ویژه می‌تواند:\n"
            "• sticker\n"
            "• animation (گیف)\n"
            "• video_note (ویدیو مسیج دایره‌ای)\n"
            "ارسال کند.\n"
            "عکس و ویدیو برای همه حذف می‌شود.",
            reply_markup=kb_special_menu(),
        )
        return

    if data == "special:list":
        await callback.message.edit_text(await fmt_specials_text(), reply_markup=kb_special_menu())
        return

    if data == "admin:add":
        if not is_owner:
            await callback.message.edit_text("فقط ادمین اصلی می‌تواند ادمین اضافه کند.", reply_markup=kb_main_menu(is_owner))
            return
        await db_set_admin_state(user_id, "await_admin_add")
        await callback.message.edit_text(
            "ID عددی ادمین را بفرست یا یک پیام/فوروارد از همان کاربر ارسال کن.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "admin:list":
        if not is_owner:
            await callback.message.edit_text("فقط ادمین اصلی می‌تواند لیست ادمین‌ها را ببیند.", reply_markup=kb_main_menu(is_owner))
            return
        await callback.message.edit_text(await fmt_admins_text(), reply_markup=kb_admin_menu())
        return

    if data == "admin:remove":
        if not is_owner:
            await callback.message.edit_text("فقط ادمین اصلی می‌تواند ادمین حذف کند.", reply_markup=kb_main_menu(is_owner))
            return
        await db_set_admin_state(user_id, "await_admin_remove")
        await callback.message.edit_text(
            "ID عددی ادمین را برای حذف بفرست.\n"
            "برای لغو: /cancel"
        )
        return


# =====================
# دستورات ویژه در گروه
# =====================

@router.message(Command("special"))
async def special_add_slash_handler(message: Message):
    await process_special_command(message, add=True)


@router.message(Command("unspecial"))
async def special_remove_slash_handler(message: Message):
    await process_special_command(message, add=False)


async def process_special_command(message: Message, add: bool) -> None:
    target_chat_id = await get_allowed_target_chat_id()
    if message.chat.type == "private":
        return
    if target_chat_id is None or message.chat.id != target_chat_id:
        return
    if not message.from_user or not await db_is_admin(message.from_user.id):
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply("برای این کار، باید روی پیام کاربر ریپلای کنی.")
        return

    user = message.reply_to_message.from_user
    if add:
        await db_add_special_user(user.id, user.first_name, user.username)
        await message.reply(f"کاربر ویژه شد:\n{user.full_name} ({user.id})")
    else:
        removed = await db_remove_special_user(user.id)
        if removed:
            await message.reply(f"ویژه حذف شد:\n{user.full_name} ({user.id})")
        else:
            await message.reply("این کاربر در لیست ویژه نبود.")


# =====================
# پردازش پیام‌ها در گروه
# =====================

@router.message()
async def message_router(message: Message, bot: Bot):
    # چت خصوصی: فقط پنل ادمین‌ها
    if message.chat.type == "private":
        handled = await maybe_handle_admin_private(message, bot)
        if not handled and message.from_user and await db_is_admin(message.from_user.id) and message.text:
            await message.answer("برای مدیریت بات /panel را بزن.")
        return

    # فقط روی گروهِ تنظیم‌شده
    target_chat_id = await get_allowed_target_chat_id()
    if target_chat_id is None or message.chat.id != target_chat_id:
        return

    if not message.from_user or message.from_user.is_bot:
        return

    # عضو را برای tag all ذخیره می‌کنیم
    await db_add_group_member(
        user_id=message.from_user.id,
        first_name=message.from_user.first_name,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    text = message.text or ""
    normalized = normalize_action_text(text)

    # «قوانین» برای همه، حتی ادمین اصلی و ادمین‌های اضافه‌شده
    if text and is_rules_trigger(text):
        await message.reply(await get_rules_text())
        return

    is_admin = await db_is_admin(message.from_user.id)
    special = await db_is_special_user(message.from_user.id)

    # «تگ» برای ادمین‌ها و کاربران ویژه
    if (is_admin or special) and normalized == "تگ":
        await send_tag_all(bot, message)
        return

    # تنظیم/حذف ویژه با ریپلای (فقط ادمین‌ها)
    if is_admin and normalized in {"تنظیم ویژه", "حذف ویژه"}:
        if not message.reply_to_message or not message.reply_to_message.from_user:
            await message.reply("برای این کار باید روی پیام کاربر ریپلای کنی.")
            return

        target_user = message.reply_to_message.from_user
        if normalized == "تنظیم ویژه":
            await db_add_special_user(target_user.id, target_user.first_name, target_user.username)
            await message.reply(f"کاربر ویژه شد:\n{target_user.full_name} ({target_user.id})")
        else:
            removed = await db_remove_special_user(target_user.id)
            if removed:
                await message.reply(f"ویژه حذف شد:\n{target_user.full_name} ({target_user.id})")
            else:
                await message.reply("این کاربر در لیست ویژه نبود.")
        return

    # از اینجا به بعد، قوانین ضد پیام روی همه اعمال می‌شود
    is_owner_sender = message.from_user.id == OWNER_ID

    if has_disallowed_user_identifier(message):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    # لینک‌ها: حذف می‌شوند مگر لینک استارت ربات مجاز باشد یا فرستنده ادمین اصلی باشد
    if not is_owner_sender and await has_disallowed_link(message):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    if await has_filter_match(message):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    if is_blocked_media(message, special=special):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    # ویس و متن مجاز هستند


async def send_tag_all(bot: Bot, message: Message) -> None:
    rows = await db_list_group_members()
    user_ids = [int(r["user_id"]) for r in rows if r["user_id"] != OWNER_ID]

    if not user_ids:
        await message.reply("هنوز عضوی برای منشن ثبت نشده است.")
        return

    for body in build_tag_messages(user_ids):
        await message.answer(body, parse_mode="HTML")


# =====================
# وب سرویس برای Render
# =====================

async def health(_request: web.Request) -> web.Response:
    return web.Response(text="ok", content_type="text/plain")


async def start_http_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    log.info("HTTP server started on port %s", PORT)
    return runner


# =====================
# main
# =====================

async def main() -> None:
    global db_pool, runtime_bot_username

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=5,
        ssl="require",
    )
    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    runtime_bot_username = me.username or ALLOWED_BOT_USERNAME or ""
    log.info("Bot started as @%s", runtime_bot_username or "unknown")

    runner = await start_http_server()

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await runner.cleanup()
        await bot.session.close()
        if db_pool is not None:
            await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
