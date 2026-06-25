import os
import logging
import asyncio
import threading
import json
import urllib.request
import csv
import io
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN", "").strip()
PLATFORM_URL = os.getenv("PLATFORM_URL", "https://shop.kentavr.world/").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS    = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DB_PATH      = "kentavr_stats.db"

SCREEN_IMAGES = {
    "main":    "https://i.postimg.cc/RFsmw06x/Chat-GPT-Image-4-iun-2026-g-06-12-26.png",
    "buyer":   "https://i.postimg.cc/wTSw7dBP/Chat-GPT-Image-4-iun-2026-g-06-35-43.png",
    "seller":  "https://i.postimg.cc/TwFDCfFH/IMG-20260604-035610-329.png",
    "partner": "https://i.postimg.cc/fT5gqd27/Chat-GPT-Image-4-iun-2026-g-03-57-21.png",
}

SCREEN_NAMES = {
    "main":    "🏠 Главный экран",
    "buyer":   "🛒 Покупатель",
    "seller":  "🏪 Продавец",
    "partner": "💎 Партнёр / ТТК",
}

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

db_pool     = None
USE_POSTGRES = False


# ── Health ─────────────────────────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *args): pass

def _start_health_server():
    HTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), _HealthHandler).serve_forever()


# ── Database ───────────────────────────────────────────────────────────────────

async def init_db():
    global db_pool, USE_POSTGRES
    if DATABASE_URL:
        try:
            import asyncpg
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            async with db_pool.acquire() as conn:
                await conn.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER DEFAULT 0)")
                await conn.execute("CREATE TABLE IF NOT EXISTS unique_users (user_id BIGINT PRIMARY KEY, first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, actions_count INTEGER DEFAULT 0)")
                await conn.execute("CREATE TABLE IF NOT EXISTS screen_content (screen_key TEXT PRIMARY KEY, custom_text TEXT, custom_image TEXT)")
                for key in ("starts", "buyer_opens", "seller_opens", "partner_opens"):
                    await conn.execute("INSERT INTO stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING", key)
            USE_POSTGRES = True
            logger.info("✅ PostgreSQL")
            return
        except Exception as e:
            logger.warning(f"PostgreSQL failed: {e}")

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS unique_users (user_id INTEGER PRIMARY KEY, first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, actions_count INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS screen_content (screen_key TEXT PRIMARY KEY, custom_text TEXT, custom_image TEXT)")
        for key in ("starts", "buyer_opens", "seller_opens", "partner_opens"):
            await db.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,))
        await db.commit()
    logger.info("✅ SQLite")


async def _pg(query, *args):
    async with db_pool.acquire() as conn:
        return await conn.execute(query, *args)

async def _pg_val(query, *args):
    async with db_pool.acquire() as conn:
        return await conn.fetchval(query, *args)

async def _pg_row(query, *args):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(query, *args)

async def _pg_all(query, *args):
    async with db_pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def increment_stat(key: str):
    try:
        if USE_POSTGRES and db_pool:
            await _pg("INSERT INTO stats (key,value) VALUES ($1,1) ON CONFLICT(key) DO UPDATE SET value=stats.value+1", key)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT INTO stats(key,value) VALUES(?,1) ON CONFLICT(key) DO UPDATE SET value=value+1", (key,))
                await db.commit()
    except Exception:
        pass


async def register_user(user_id: int):
    try:
        if USE_POSTGRES and db_pool:
            exists = await _pg_val("SELECT user_id FROM unique_users WHERE user_id=$1", user_id)
            if exists:
                await _pg("UPDATE unique_users SET last_seen=CURRENT_TIMESTAMP, actions_count=actions_count+1 WHERE user_id=$1", user_id)
            else:
                await _pg("INSERT INTO unique_users(user_id,first_seen,last_seen,actions_count) VALUES($1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)", user_id)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                row = await (await db.execute("SELECT user_id FROM unique_users WHERE user_id=?", (user_id,))).fetchone()
                if row:
                    await db.execute("UPDATE unique_users SET last_seen=CURRENT_TIMESTAMP,actions_count=actions_count+1 WHERE user_id=?", (user_id,))
                else:
                    await db.execute("INSERT INTO unique_users(user_id,first_seen,last_seen,actions_count) VALUES(?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)", (user_id,))
                await db.commit()
    except Exception:
        pass


async def get_stats() -> dict:
    try:
        if USE_POSTGRES and db_pool:
            rows  = await _pg_all("SELECT key,value FROM stats")
            stats = {r['key']: r['value'] for r in rows}
            stats["unique_users"]  = await _pg_val("SELECT COUNT(*) FROM unique_users") or 0
            stats["active_today"]  = await _pg_val("SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen)=CURRENT_DATE") or 0
            stats["total_actions"] = await _pg_val("SELECT SUM(actions_count) FROM unique_users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                rows  = await (await db.execute("SELECT key,value FROM stats")).fetchall()
                stats = {r[0]: r[1] for r in rows}
                stats["unique_users"]  = (await (await db.execute("SELECT COUNT(*) FROM unique_users")).fetchone())[0] or 0
                stats["active_today"]  = (await (await db.execute("SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen)=DATE('now')")).fetchone())[0] or 0
                r = await (await db.execute("SELECT SUM(actions_count) FROM unique_users")).fetchone()
                stats["total_actions"] = r[0] if r and r[0] else 0
        return stats
    except Exception:
        return {}


async def get_all_user_ids() -> list:
    try:
        if USE_POSTGRES and db_pool:
            return [r['user_id'] for r in await _pg_all("SELECT user_id FROM unique_users")]
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            return [r[0] for r in await (await db.execute("SELECT user_id FROM unique_users")).fetchall()]
    except Exception:
        return []


async def get_custom_text(key: str):
    try:
        if USE_POSTGRES and db_pool:
            r = await _pg_row("SELECT custom_text FROM screen_content WHERE screen_key=$1", key)
            return r['custom_text'] if r else None
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            r = await (await db.execute("SELECT custom_text FROM screen_content WHERE screen_key=?", (key,))).fetchone()
            return r[0] if r else None
    except Exception:
        return None


async def get_custom_image(key: str):
    try:
        if USE_POSTGRES and db_pool:
            r = await _pg_row("SELECT custom_image FROM screen_content WHERE screen_key=$1", key)
            return r['custom_image'] if r else None
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            r = await (await db.execute("SELECT custom_image FROM screen_content WHERE screen_key=?", (key,))).fetchone()
            return r[0] if r else None
    except Exception:
        return None


async def save_custom_text(key: str, text: str):
    if USE_POSTGRES and db_pool:
        await _pg("INSERT INTO screen_content(screen_key,custom_text) VALUES($1,$2) ON CONFLICT(screen_key) DO UPDATE SET custom_text=$2", key, text)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO screen_content(screen_key,custom_text) VALUES(?,?) ON CONFLICT(screen_key) DO UPDATE SET custom_text=?", (key, text, text))
            await db.commit()


async def save_custom_image(key: str, url: str):
    if USE_POSTGRES and db_pool:
        await _pg("INSERT INTO screen_content(screen_key,custom_image) VALUES($1,$2) ON CONFLICT(screen_key) DO UPDATE SET custom_image=$2", key, url)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO screen_content(screen_key,custom_image) VALUES(?,?) ON CONFLICT(screen_key) DO UPDATE SET custom_image=?", (key, url, url))
            await db.commit()


async def reset_custom_text(key: str):
    if USE_POSTGRES and db_pool:
        await _pg("UPDATE screen_content SET custom_text=NULL WHERE screen_key=$1", key)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE screen_content SET custom_text=NULL WHERE screen_key=?", (key,))
            await db.commit()


async def reset_custom_image(key: str):
    if USE_POSTGRES and db_pool:
        await _pg("UPDATE screen_content SET custom_image=NULL WHERE screen_key=$1", key)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE screen_content SET custom_image=NULL WHERE screen_key=?", (key,))
            await db.commit()


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


# ── Screens ────────────────────────────────────────────────────────────────────

def screen_main():
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b> — социальный маркетплейс, где "
        "покупатели, продавцы и партнёры работают в единой системе взаимной выгоды.\n\n"
        "<i>Кем ты являешься?</i>"
    )
    kb = [
        [InlineKeyboardButton("🛒 Я покупатель",          callback_data="screen:buyer")],
        [InlineKeyboardButton("🏪 Я продавец",            callback_data="screen:seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром",  callback_data="screen:partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу",  url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url="https://kentavrmarket.shop"))],
    ]
    return text, InlineKeyboardMarkup(kb)


def screen_buyer():
    text = (
        "🛒 <b>Для покупателей</b>\n\n"
        "Покупай у проверенных участников сообщества и получай <b>кэшбэк в ТТК</b> "
        "за каждую покупку.\n\n"
        "Чем активнее ты покупаешь — тем больше возможностей открывается."
    )
    kb = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню",         callback_data="screen:main")],
    ]
    return text, InlineKeyboardMarkup(kb)


def screen_seller():
    text = (
        "🏪 <b>Для продавцов</b>\n\n"
        "Размести товары, услуги или экспертизу и получи доступ к активной аудитории.\n\n"
        "Здесь строят <b>долгосрочные отношения</b>, а не гонятся за разовыми сделками."
    )
    kb = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню",         callback_data="screen:main")],
    ]
    return text, InlineKeyboardMarkup(kb)


def screen_partner():
    text = (
        "💎 <b>Партнёр / ТТК</b>\n\n"
        "Участвуй в развитии платформы и получай <b>Торговый Токен KENTAVR (ТТК)</b>.\n\n"
        "Его ценность растёт вместе с товарооборотом сообщества — "
        "это не биржевая крипта, а реальная внутренняя экономика."
    )
    kb = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню",         callback_data="screen:main")],
    ]
    return text, InlineKeyboardMarkup(kb)


SCREENS = {
    "main":    screen_main,
    "buyer":   screen_buyer,
    "seller":  screen_seller,
    "partner": screen_partner,
}
STAT_MAP = {"buyer": "buyer_opens", "seller": "seller_opens", "partner": "partner_opens"}


# ── Render ─────────────────────────────────────────────────────────────────────

async def send_screen(bot, chat_id: int, screen_key: str):
    builder = SCREENS.get(screen_key, screen_main)
    default_text, markup = builder()

    custom_text  = await get_custom_text(screen_key)
    custom_image = await get_custom_image(screen_key)
    text      = custom_text  if custom_text  else default_text
    image_url = custom_image if custom_image else SCREEN_IMAGES.get(screen_key)

    if image_url:
        try:
            await bot.send_photo(chat_id=chat_id, photo=image_url, caption=text,
                                 reply_markup=markup, parse_mode="HTML")
            return
        except Exception as e:
            logger.warning(f"send_photo failed: {e}")

    await bot.send_message(chat_id=chat_id, text=text,
                           reply_markup=markup, parse_mode="HTML")


async def delete_msg(bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


# ── Admin panel ────────────────────────────────────────────────────────────────

async def send_admin_panel(bot, chat_id: int, message_id: int = None):
    stats = await get_stats()
    text = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n"
        "<i>KENTAVR MARKET Bot</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{stats.get('unique_users', 0)}</b>   "
        f"📅 Сегодня: <b>{stats.get('active_today', 0)}</b>\n"
        f"🎯 Всего действий: <b>{stats.get('total_actions', 0)}</b>\n\n"
        "<i>Выбери действие:</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
         InlineKeyboardButton("👥 Пользователи", callback_data="adm:users")],
        [InlineKeyboardButton("📢 Рассылка",     callback_data="adm:broadcast"),
         InlineKeyboardButton("📥 Экспорт CSV",  callback_data="adm:export")],
        [InlineKeyboardButton("✏️ Изменить тексты",   callback_data="adm:edit_texts")],
        [InlineKeyboardButton("🖼 Изменить картинки",  callback_data="adm:edit_images")],
        [InlineKeyboardButton("🏠 На главную",         callback_data="screen:main")],
    ])
    if message_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")


# ── Handlers ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    uid = update.effective_user.id
    await asyncio.gather(increment_stat("starts"), register_user(uid))
    await send_screen(context.bot, update.effective_chat.id, "main")


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    context.user_data.clear()
    await send_admin_panel(context.bot, update.effective_chat.id)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query  = update.callback_query
    await query.answer()
    data   = query.data
    bot    = context.bot
    chat_id = update.effective_chat.id
    uid    = update.effective_user.id
    msg_id = query.message.message_id

    # ── Screen navigation ──────────────────────────────────────────────────────
    if data.startswith("screen:"):
        key = data[len("screen:"):]
        context.user_data.clear()
        if key in STAT_MAP:
            await increment_stat(STAT_MAP[key])
        await delete_msg(bot, chat_id, msg_id)
        await send_screen(bot, chat_id, key)
        return

    # ── Admin only below ───────────────────────────────────────────────────────
    if not is_admin(uid):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return

    # Admin panel
    if data == "adm:panel":
        context.user_data.clear()
        await send_admin_panel(bot, chat_id, msg_id)

    elif data == "adm:stats":
        s = await get_stats()
        text = (
            "📊 <b>СТАТИСТИКА</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Пользователей: <code>{s.get('unique_users',0)}</code>\n"
            f"📅 Активных сегодня: <code>{s.get('active_today',0)}</code>\n"
            f"🎯 Всего действий: <code>{s.get('total_actions',0)}</code>\n\n"
            f"▶️ /start: <code>{s.get('starts',0)}</code>\n"
            f"🛒 Покупатель: <code>{s.get('buyer_opens',0)}</code>\n"
            f"🏪 Продавец: <code>{s.get('seller_opens',0)}</code>\n"
            f"💎 Партнёр: <code>{s.get('partner_opens',0)}</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:users":
        users = await get_all_user_ids()
        text  = f"👥 <b>Пользователи</b>\nВсего: <code>{len(users)}</code>\n\nПоследние 20:\n"
        for i, u in enumerate(users[-20:][::-1], 1):
            text += f"{i}. <code>{u}</code>\n"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:export":
        users = await get_all_user_ids()
        s     = await get_stats()
        out   = io.StringIO()
        w     = csv.writer(out)
        w.writerow(['Метрика', 'Значение'])
        for k, lbl in [('unique_users','Пользователей'),('active_today','Сегодня'),
                        ('total_actions','Действий'),('starts','/start'),
                        ('buyer_opens','Покупатель'),('seller_opens','Продавец'),
                        ('partner_opens','Партнёр')]:
            w.writerow([lbl, s.get(k, 0)])
        w.writerow([]); w.writerow(['ID'])
        for u in users:
            w.writerow([u])
        out.seek(0)
        await bot.send_document(
            chat_id=chat_id,
            document=io.BytesIO(out.getvalue().encode()),
            filename=f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            caption="📊 Экспорт KENTAVR MARKET",
        )

    elif data == "adm:broadcast":
        users = await get_all_user_ids()
        context.user_data['state'] = 'broadcast'
        text = (
            f"📢 <b>Рассылка</b>\n\nАудитория: <b>{len(users)} чел.</b>\n\n"
            "Отправь текст сообщения (HTML поддерживается).\n/cancel — отмена"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:edit_texts":
        context.user_data['state'] = 'edit_texts_menu'
        kb_rows = [[InlineKeyboardButton(name, callback_data=f"adm:pick_text:{key}")]
                   for key, name in SCREEN_NAMES.items()]
        kb_rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        kb = InlineKeyboardMarkup(kb_rows)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text="✏️ <b>Тексты экранов</b>\n\nВыбери экран:",
                                        reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id,
                                   text="✏️ <b>Тексты экранов</b>\n\nВыбери экран:",
                                   reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:pick_text:"):
        screen_key = data[len("adm:pick_text:"):]
        context.user_data['state']      = 'edit_text'
        context.user_data['edit_screen'] = screen_key

        custom_text = await get_custom_text(screen_key)
        default_text, _ = SCREENS.get(screen_key, screen_main)()
        current = custom_text if custom_text else default_text
        label   = "изменён ✏️" if custom_text else "оригинал"
        preview = current[:500] + ("…" if len(current) > 500 else "")

        text = (
            f"✏️ <b>{SCREEN_NAMES.get(screen_key, screen_key)}</b> ({label})\n\n"
            f"<b>Текущий текст:</b>\n<blockquote>{preview}</blockquote>\n\n"
            "Отправь новый текст. /cancel — отмена"
        )
        kb_rows = []
        if custom_text:
            kb_rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:reset_text:{screen_key}")])
        kb_rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:edit_texts")])
        kb = InlineKeyboardMarkup(kb_rows)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:reset_text:"):
        screen_key = data[len("adm:reset_text:"):]
        await reset_custom_text(screen_key)
        context.user_data.clear()
        name = SCREEN_NAMES.get(screen_key, screen_key)
        kb   = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=f"🔄 Текст <b>{name}</b> сброшен к оригиналу.",
                                        reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=f"🔄 Текст <b>{name}</b> сброшен.",
                                   reply_markup=kb, parse_mode="HTML")

    elif data == "adm:edit_images":
        context.user_data['state'] = 'edit_images_menu'
        kb_rows = [[InlineKeyboardButton(SCREEN_NAMES[k], callback_data=f"adm:pick_image:{k}")]
                   for k in ["main", "buyer", "seller", "partner"]]
        kb_rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        kb = InlineKeyboardMarkup(kb_rows)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text="🖼 <b>Картинки экранов</b>\n\nВыбери экран:",
                                        reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id,
                                   text="🖼 <b>Картинки экранов</b>\n\nВыбери экран:",
                                   reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:pick_image:"):
        screen_key = data[len("adm:pick_image:"):]
        context.user_data['state']      = 'edit_image'
        context.user_data['edit_screen'] = screen_key

        custom_img = await get_custom_image(screen_key)
        current    = custom_img if custom_img else SCREEN_IMAGES.get(screen_key, "нет")
        label      = "изменена ✏️" if custom_img else "оригинал"

        text = (
            f"🖼 <b>{SCREEN_NAMES.get(screen_key, screen_key)}</b> ({label})\n\n"
            f"<b>Текущая:</b>\n<code>{current}</code>\n\n"
            "Отправь новую ссылку (https://…). /cancel — отмена"
        )
        kb_rows = []
        if custom_img:
            kb_rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:reset_image:{screen_key}")])
        kb_rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:edit_images")])
        kb = InlineKeyboardMarkup(kb_rows)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:reset_image:"):
        screen_key = data[len("adm:reset_image:"):]
        await reset_custom_image(screen_key)
        context.user_data.clear()
        name = SCREEN_NAMES.get(screen_key, screen_key)
        kb   = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id,
                                        text=f"🔄 Картинка <b>{name}</b> сброшена к оригиналу.",
                                        reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=chat_id, text=f"🔄 Картинка <b>{name}</b> сброшена.",
                                   reply_markup=kb, parse_mode="HTML")


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = context.user_data.get('state')

    if text.strip() == "/cancel":
        context.user_data.clear()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        await update.message.reply_text("❌ Отменено.", reply_markup=kb)
        return

    if not is_admin(uid) or not state:
        return

    if state == 'broadcast':
        users  = await get_all_user_ids()
        sent   = failed = 0
        status = await update.message.reply_text(f"⏳ Отправляю {len(users)} пользователям...")
        for i, u in enumerate(users):
            try:
                await context.bot.send_message(u, text, parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            if (i + 1) % 30 == 0:
                await asyncio.sleep(1)
        context.user_data.clear()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        await status.edit_text(
            f"✅ Доставлено: <b>{sent}</b>\n❌ Ошибок: <b>{failed}</b>",
            reply_markup=kb, parse_mode="HTML"
        )

    elif state == 'edit_text':
        screen_key = context.user_data.get('edit_screen')
        if screen_key:
            await save_custom_text(screen_key, text)
            name = SCREEN_NAMES.get(screen_key, screen_key)
            context.user_data.clear()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:edit_texts")],
                [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
            ])
            await update.message.reply_text(f"✅ Текст <b>{name}</b> обновлён!",
                                            reply_markup=kb, parse_mode="HTML")

    elif state == 'edit_image':
        screen_key = context.user_data.get('edit_screen')
        if screen_key:
            url = text.strip()
            if not url.startswith("http"):
                await update.message.reply_text("❌ Ссылка должна начинаться с https://. Попробуй ещё раз:")
                return
            await save_custom_image(screen_key, url)
            name = SCREEN_NAMES.get(screen_key, screen_key)
            context.user_data.clear()
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🖼 Изменить ещё", callback_data="adm:edit_images")],
                [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
            ])
            await update.message.reply_text(f"✅ Картинка <b>{name}</b> обновлена!",
                                            reply_markup=kb, parse_mode="HTML")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}", exc_info=context.error)


# ── Main ───────────────────────────────────────────────────────────────────────

async def async_main():
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT, on_message))

    logger.info("🚀 Бот KENTAVR MARKET запущен!")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    try:
        while True:
            await asyncio.sleep(3600)
    except Exception:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не установлен!")
    if not ADMIN_IDS:
        logger.warning("⚠️ ADMIN_IDS не установлен!")
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            data=json.dumps({"drop_pending_updates": True}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass
    threading.Thread(target=_start_health_server, daemon=True).start()
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
