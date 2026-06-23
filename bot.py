#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Guardian Telegram Bot (Web Service + Polling)
- فقط در یک گروهِ تعیین‌شده فعال می‌شود
- پیام‌های عکس / ویدیو / گیف / استیکر / ویدیو نوت را حذف می‌کند
- فقط ویس را اجازه می‌دهد
- لینک‌ها را حذف می‌کند، به‌جز لینک‌های start برای همین بات
- فیلتر کلمات دارد
- کاربران ویژه (با ریپلای ادمین) می‌توانند sticker / animation / video_note بفرستند
- همه تنظیمات در Neon/PostgreSQL ذخیره می‌شود
- بدون webhook اجرا می‌شود، ولی برای Render Web Service یک HTTP health endpoint دارد

اجرا:
  pip install -r requirements.txt
  export BOT_TOKEN="123456:ABC..."
  export DATABASE_URL="postgresql://..."
  python guardian_bot_web.py

نکته:
  بات باید در گروه هدف ادمین باشد و اجازه حذف پیام داشته باشد.
"""

import asyncio
import json
import logging
import os
import re
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

ADMIN_ID = int(os.getenv("OWNER_ID", "8883527571"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PORT = int(os.getenv("PORT", "10000"))

# این مقدار در startup از خود بات خوانده می‌شود؛ فقط به‌عنوان fallback است
ALLOWED_BOT_USERNAME = os.getenv("ALLOWED_BOT_USERNAME", "").strip()

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
# ابزارهای دیتابیس
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
            CREATE TABLE IF NOT EXISTS admin_state (
                admin_id BIGINT PRIMARY KEY,
                state TEXT,
                payload JSONB
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


async def db_set_admin_state(state: Optional[str], payload: Optional[dict[str, Any]] = None) -> None:
    if state is None:
        await db_exec("DELETE FROM admin_state WHERE admin_id = $1", ADMIN_ID)
        return

    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    await db_exec(
        """
        INSERT INTO admin_state(admin_id, state, payload) VALUES($1, $2, $3::jsonb)
        ON CONFLICT(admin_id) DO UPDATE SET
            state = EXCLUDED.state,
            payload = EXCLUDED.payload
        """,
        ADMIN_ID,
        state,
        payload_json,
    )


async def db_get_admin_state() -> tuple[Optional[str], dict[str, Any]]:
    row = await db_fetchrow(
        "SELECT state, payload FROM admin_state WHERE admin_id = $1",
        ADMIN_ID,
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


# =====================
# ابزارها
# =====================

def normalize_filter(word: str) -> str:
    return re.sub(r"\s+", " ", (word or "").strip().casefold())


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().casefold())


def safe_int(text: str) -> Optional[int]:
    try:
        return int(str(text).strip())
    except Exception:
        return None


def is_rules_trigger(text: str) -> bool:
    return bool(re.fullmatch(r"\s*قوانین\s*", text or ""))


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


def is_allowed_bot_start_link(url: str) -> bool:
    if not url:
        return False

    url = url.strip().rstrip(".,;!؟?)]}»\"'")
    username = runtime_bot_username or ALLOWED_BOT_USERNAME
    if not username:
        return False

    pattern = re.compile(
        rf"^(?:https?://)?(?:t\.me|telegram\.me)/{re.escape(username)}\?start=[A-Za-z0-9_\-]+$",
        re.IGNORECASE,
    )
    return bool(pattern.fullmatch(url))


def has_disallowed_link(message: Message) -> bool:
    for _, text, ent in iter_message_entities(message):
        if ent.type not in {MessageEntityType.URL, MessageEntityType.TEXT_LINK}:
            continue
        url = entity_url(text, ent)
        if not url:
            continue
        if not is_allowed_bot_start_link(url):
            return True
    return False


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


async def delete_message_safe(bot: Bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        log.warning("delete_message failed chat=%s msg=%s err=%s", chat_id, message_id, e)


async def is_user_chat_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
    except Exception:
        return False


def kb_main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="تنظیم گروه", callback_data="menu:set_group")
    kb.button(text="تنظیمات فیلتر", callback_data="menu:filters")
    kb.button(text="متن قوانین", callback_data="menu:rules")
    kb.button(text="لیست کاربران ویژه", callback_data="menu:specials")
    kb.button(text="گروه فعال", callback_data="menu:show_group")
    kb.adjust(2, 2, 1)
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
        lines.append(f"• {name} | {username} | {r['user_id']}")
    return "کاربران ویژه:\n" + "\n".join(lines)


async def get_allowed_target_chat_id() -> Optional[int]:
    val = await db_get_setting("target_chat_id")
    return safe_int(val) if val else None


async def get_rules_text() -> str:
    return await db_get_setting("rules_text", "متن قوانین هنوز تنظیم نشده است.") or "متن قوانین هنوز تنظیم نشده است."


# =====================
# پردازش چت خصوصی ادمین
# =====================

async def maybe_handle_admin_private(message: Message, bot: Bot) -> bool:
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return False

    state, _payload = await db_get_admin_state()
    text = (message.text or "").strip()

    if text in {"/start", "/panel", "پنل", "منو"} and not state:
        await message.answer(
            "پنل مدیریت بات:\n"
            "از دکمه‌ها برای تنظیم گروه، فیلترها، متن قوانین و کاربران ویژه استفاده کن.",
            reply_markup=kb_main_menu(),
        )
        return True

    if text == "/cancel":
        await db_set_admin_state(None)
        await message.answer("عملیات جاری لغو شد.", reply_markup=kb_main_menu())
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

        await db_set_admin_state(None)
        await message.answer(
            f"گروه با موفقیت تنظیم شد.\n{await fmt_group_text()}",
            reply_markup=kb_main_menu(),
        )
        return True

    if state == "await_rules_text":
        rules_text = (message.text or message.caption or "").strip()
        if not rules_text:
            await message.answer("متن قوانین خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        await db_set_setting("rules_text", rules_text)
        await db_set_admin_state(None)
        await message.answer("متن قوانین ذخیره شد.", reply_markup=kb_main_menu())
        return True

    if state == "await_filter_add":
        word = normalize_filter(text)
        if not word:
            await message.answer("فیلتر خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        added = await db_add_filter(word)
        await db_set_admin_state(None)
        msg = f"فیلتر اضافه شد: {word}" if added else f"این فیلتر از قبل وجود دارد: {word}"
        await message.answer(msg, reply_markup=kb_main_menu())
        return True

    if state == "await_filter_remove":
        word = normalize_filter(text)
        if not word:
            await message.answer("فیلتر خالی است. دوباره بفرست.", reply_markup=kb_back_home())
            return True

        removed = await db_remove_filter(word)
        await db_set_admin_state(None)
        msg = f"فیلتر حذف شد: {word}" if removed else f"این فیلتر پیدا نشد: {word}"
        await message.answer(msg, reply_markup=kb_main_menu())
        return True

    if text == "/start":
        await message.answer("برای مدیریت بات /panel را بزن.", reply_markup=kb_main_menu())
        return True

    return False


# =====================
# کال‌بک‌ها
# =====================

@router.callback_query(F.from_user.id == ADMIN_ID)
async def callback_handler(callback: CallbackQuery):
    data = callback.data or ""
    await callback.answer()

    if data == "menu:home":
        await db_set_admin_state(None)
        await callback.message.edit_text(
            "پنل مدیریت بات:\n"
            "از دکمه‌ها برای تنظیم گروه، فیلترها، متن قوانین و کاربران ویژه استفاده کن.",
            reply_markup=kb_main_menu(),
        )
        return

    if data == "menu:show_group":
        await callback.message.edit_text(await fmt_group_text(), reply_markup=kb_main_menu())
        return

    if data == "menu:set_group":
        await db_set_admin_state("await_group_id")
        await callback.message.edit_text(
            "حالا ID گروه را بفرست یا یک پیام فوروارد شده از همان گروه را ارسال کن.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "menu:rules":
        await db_set_admin_state("await_rules_text")
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
            "در گروهِ هدف روی پیام کاربر ریپلای کن و /special یا /unspecial بفرست.\n"
            "کاربر ویژه فقط استیکر، گیف و ویدیو نوت را می‌تواند بفرستد.",
            reply_markup=kb_special_menu(),
        )
        return

    if data == "filter:add":
        await db_set_admin_state("await_filter_add")
        await callback.message.edit_text(
            "کلمه یا عبارت فیلتر جدید را بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "filter:list":
        await callback.message.edit_text(await fmt_filters_text(), reply_markup=kb_filter_menu())
        return

    if data == "filter:remove":
        await db_set_admin_state("await_filter_remove")
        await callback.message.edit_text(
            "کلمه یا عبارت فیلتر را برای حذف بفرست.\n"
            "برای لغو: /cancel"
        )
        return

    if data == "special:help":
        await callback.message.edit_text(
            "روش استفاده:\n"
            "1) در گروه هدف روی پیام کاربر ریپلای کن.\n"
            "2) /special بفرست تا ویژه شود.\n"
            "3) /unspecial بفرست تا ویژه حذف شود.\n\n"
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


# =====================
# دستورات ویژه
# =====================

@router.message(Command("special"))
async def special_add_handler(message: Message):
    target_chat_id = await get_allowed_target_chat_id()
    if message.chat.type == "private":
        return
    if target_chat_id is None or message.chat.id != target_chat_id:
        return
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply("برای ویژه کردن، باید روی پیام کاربر ریپلای کنی.")
        return

    user = message.reply_to_message.from_user
    await db_add_special_user(user.id, user.first_name, user.username)
    await message.reply(f"کاربر ویژه شد:\n{user.full_name} ({user.id})")


@router.message(Command("unspecial"))
async def special_remove_handler(message: Message):
    target_chat_id = await get_allowed_target_chat_id()
    if message.chat.type == "private":
        return
    if target_chat_id is None or message.chat.id != target_chat_id:
        return
    if not message.from_user or message.from_user.id != ADMIN_ID:
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply("برای حذف ویژه، باید روی پیام کاربر ریپلای کنی.")
        return

    user = message.reply_to_message.from_user
    removed = await db_remove_special_user(user.id)
    if removed:
        await message.reply(f"ویژه حذف شد:\n{user.full_name} ({user.id})")
    else:
        await message.reply("این کاربر در لیست ویژه نبود.")


# =====================
# پردازش پیام‌ها
# =====================

@router.message()
async def message_router(message: Message, bot: Bot):
    # چت خصوصی: فقط ادمین اصلی
    if message.chat.type == "private":
        handled = await maybe_handle_admin_private(message, bot)
        if not handled and message.from_user and message.from_user.id == ADMIN_ID:
            if message.text:
                await message.answer("برای مدیریت بات /panel را بزن.")
        return

    # فقط روی گروهِ تنظیم‌شده
    target_chat_id = await get_allowed_target_chat_id()
    if target_chat_id is None or message.chat.id != target_chat_id:
        return

    if not message.from_user:
        return

    # ادمین اصلی آزاد است
    if message.from_user.id == ADMIN_ID:
        return

    # ادمین/creator گروه آزاد است
    if await is_user_chat_admin(bot, message.chat.id, message.from_user.id):
        return

    text = message.text or ""
    special = await db_is_special_user(message.from_user.id)

    # 1) قوانین: وقتی کسی فقط «قوانین» بنویسد، پاسخ ریپلای شود
    if text and is_rules_trigger(text):
        await message.reply(await get_rules_text())
        return

    # 2) لینک‌ها
    if has_disallowed_link(message):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    # 3) فیلتر کلمات
    if await has_filter_match(message):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    # 4) رسانه‌ها
    if is_blocked_media(message, special=special):
        await delete_message_safe(bot, message.chat.id, message.message_id)
        return

    # ویس و متن مجاز هستند


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
