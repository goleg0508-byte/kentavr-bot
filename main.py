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
    ConversationHandler,
    ContextTypes,
    filters,
)
from telegram.error import BadRequest, Forbidden, TelegramError

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PLATFORM_URL = os.getenv("PLATFORM_URL", "https://kentavr.world/?ref=kentavrmarket").strip()
LANDING_URL = os.getenv("LANDING_URL", "https://kentavr.world").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

DB_PATH = "kentavr_stats.db"

BROADCAST_WAITING = 1
EDIT_TEXT_SCREEN  = 2
EDIT_TEXT_CONTENT = 3
EDIT_IMAGE_SCREEN = 4
EDIT_IMAGE_URL    = 5

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

db_pool = None
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
                await conn.execute("CREATE TABLE IF NOT EXISTS user_sessions (user_id BIGINT PRIMARY KEY, last_screen TEXT DEFAULT 'main', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                await conn.execute("CREATE TABLE IF NOT EXISTS screen_content (screen_key TEXT PRIMARY KEY, custom_text TEXT, custom_image TEXT)")
                for key in ("starts","buyer_opens","seller_opens","ttk_opens","platform_clicks","commercial_opens"):
                    await conn.execute("INSERT INTO stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING", key)
            USE_POSTGRES = True
            logger.info("✅ PostgreSQL initialized")
            return
        except Exception as e:
            logger.warning(f"PostgreSQL failed: {e}")

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS unique_users (user_id INTEGER PRIMARY KEY, first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, actions_count INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS user_sessions (user_id INTEGER PRIMARY KEY, last_screen TEXT DEFAULT 'main', last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        await db.execute("CREATE TABLE IF NOT EXISTS screen_content (screen_key TEXT PRIMARY KEY, custom_text TEXT, custom_image TEXT)")
        for key in ("starts","buyer_opens","seller_opens","ttk_opens","platform_clicks","commercial_opens"):
            await db.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,))
        await db.commit()
    logger.info("✅ SQLite initialized")


async def increment_stat(key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO stats (key, value) VALUES ($1, 1) ON CONFLICT (key) DO UPDATE SET value = stats.value + 1", key)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO stats (key, value) VALUES (?, 1) ON CONFLICT(key) DO UPDATE SET value = value + 1", (key,))
            await db.commit()


async def register_user(user_id: int):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            existing = await conn.fetchval("SELECT user_id FROM unique_users WHERE user_id = $1", user_id)
            if existing:
                await conn.execute("UPDATE unique_users SET last_seen = CURRENT_TIMESTAMP, actions_count = actions_count + 1 WHERE user_id = $1", user_id)
            else:
                await conn.execute("INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count) VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)", user_id)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_id FROM unique_users WHERE user_id = ?", (user_id,))
            if await cursor.fetchone():
                await db.execute("UPDATE unique_users SET last_seen = CURRENT_TIMESTAMP, actions_count = actions_count + 1 WHERE user_id = ?", (user_id,))
            else:
                await db.execute("INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count) VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)", (user_id,))
            await db.commit()


async def save_user_session(user_id: int, screen: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO user_sessions (user_id, last_screen, last_updated) VALUES ($1, $2, CURRENT_TIMESTAMP) ON CONFLICT (user_id) DO UPDATE SET last_screen = $2, last_updated = CURRENT_TIMESTAMP", user_id, screen)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO user_sessions (user_id, last_screen, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET last_screen = ?, last_updated = CURRENT_TIMESTAMP", (user_id, screen, screen))
            await db.commit()


async def get_stats() -> dict:
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM stats")
            stats = {row['key']: row['value'] for row in rows}
            stats["unique_users"]  = await conn.fetchval("SELECT COUNT(*) FROM unique_users") or 0
            stats["active_today"]  = await conn.fetchval("SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen) = CURRENT_DATE") or 0
            stats["total_actions"] = await conn.fetchval("SELECT SUM(actions_count) FROM unique_users") or 0
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT key, value FROM stats")
            stats = {row[0]: row[1] for row in await cursor.fetchall()}
            r2 = await (await db.execute("SELECT COUNT(*) FROM unique_users")).fetchone()
            stats["unique_users"] = r2[0] or 0
            r3 = await (await db.execute("SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen) = DATE('now')")).fetchone()
            stats["active_today"] = r3[0] or 0
            r4 = await (await db.execute("SELECT SUM(actions_count) FROM unique_users")).fetchone()
            stats["total_actions"] = r4[0] if r4 and r4[0] else 0
    return stats


async def get_all_user_ids() -> list:
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            return [row['user_id'] for row in await conn.fetch("SELECT user_id FROM unique_users")]
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        return [row[0] for row in await (await db.execute("SELECT user_id FROM unique_users")).fetchall()]


async def get_custom_text(screen_key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT custom_text FROM screen_content WHERE screen_key = $1", screen_key)
            return row['custom_text'] if row else None
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute("SELECT custom_text FROM screen_content WHERE screen_key = ?", (screen_key,))).fetchone()
        return row[0] if row else None


async def get_custom_image(screen_key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT custom_image FROM screen_content WHERE screen_key = $1", screen_key)
            return row['custom_image'] if row else None
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute("SELECT custom_image FROM screen_content WHERE screen_key = ?", (screen_key,))).fetchone()
        return row[0] if row else None


async def save_custom_text(screen_key: str, text: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO screen_content (screen_key, custom_text) VALUES ($1, $2) ON CONFLICT (screen_key) DO UPDATE SET custom_text = $2", screen_key, text)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO screen_content (screen_key, custom_text) VALUES (?, ?) ON CONFLICT(screen_key) DO UPDATE SET custom_text = ?", (screen_key, text, text))
            await db.commit()


async def save_custom_image(screen_key: str, url: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO screen_content (screen_key, custom_image) VALUES ($1, $2) ON CONFLICT (screen_key) DO UPDATE SET custom_image = $2", screen_key, url)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO screen_content (screen_key, custom_image) VALUES (?, ?) ON CONFLICT(screen_key) DO UPDATE SET custom_image = ?", (screen_key, url, url))
            await db.commit()


async def reset_custom_text(screen_key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE screen_content SET custom_text = NULL WHERE screen_key = $1", screen_key)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE screen_content SET custom_text = NULL WHERE screen_key = ?", (screen_key,))
            await db.commit()


async def reset_custom_image(screen_key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE screen_content SET custom_image = NULL WHERE screen_key = $1", screen_key)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE screen_content SET custom_image = NULL WHERE screen_key = ?", (screen_key,))
            await db.commit()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ── Screens ────────────────────────────────────────────────────────────────────

def screen_main():
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b> — социальный маркетплейс, где "
        "покупатели, продавцы и партнёры работают в единой системе взаимной выгоды.\n\n"
        "<i>Кем ты являешься?</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🛒 Я покупатель",        callback_data="buyer")],
        [InlineKeyboardButton("🏪 Я продавец",          callback_data="seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром", callback_data="partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url="https://kentavrmarket.shop"))],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_buyer():
    text = (
        "🛒 <b>Для покупателей</b>\n\n"
        "Покупай у проверенных участников сообщества и получай <b>кэшбэк в ТТК</b> "
        "за каждую покупку.\n\n"
        "Чем активнее ты покупаешь — тем больше возможностей открывается."
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_seller():
    text = (
        "🏪 <b>Для продавцов</b>\n\n"
        "Размести товары, услуги или экспертизу и получи доступ к активной аудитории.\n\n"
        "Здесь строят <b>долгосрочные отношения</b>, а не гонятся за разовыми сделками."
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_partner():
    text = (
        "💎 <b>Партнёр / ТТК</b>\n\n"
        "Участвуй в развитии платформы и получай <b>Торговый Токен KENTAVR (ТТК)</b>.\n\n"
        "Его ценность растёт вместе с товарооборотом сообщества — "
        "это не биржевая крипта, а реальная внутренняя экономика."
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


SCREENS = {
    "main":    screen_main,
    "buyer":   screen_buyer,
    "seller":  screen_seller,
    "partner": screen_partner,
}
STAT_MAP = {"buyer": "buyer_opens", "seller": "seller_opens", "partner": "ttk_opens"}


# ── Render ─────────────────────────────────────────────────────────────────────

async def render_screen(screen_key: str, update: Update, context: ContextTypes.DEFAULT_TYPE, is_new: bool = False):
    user_id = update.effective_user.id
    try:
        await save_user_session(user_id, screen_key)
    except Exception:
        pass

    builder = SCREENS.get(screen_key, screen_main)
    default_text, markup = builder()

    try:
        custom_text  = await get_custom_text(screen_key)
        custom_image = await get_custom_image(screen_key)
    except Exception:
        custom_text = custom_image = None

    text      = custom_text  if custom_text  else default_text
    image_url = custom_image if custom_image else SCREEN_IMAGES.get(screen_key)

    try:
        if screen_key in STAT_MAP:
            await increment_stat(STAT_MAP[screen_key])
    except Exception:
        pass

    if is_new:
        if image_url:
            try:
                await update.message.reply_photo(photo=image_url, caption=text, reply_markup=markup, parse_mode="HTML")
                return
            except Exception:
                pass
        await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
        return

    query = update.callback_query
    chat = query.message.chat
    try:
        await query.message.delete()
    except Exception:
        pass

    if image_url:
        try:
            await chat.send_photo(photo=image_url, caption=text, reply_markup=markup, parse_mode="HTML")
            return
        except Exception:
            pass
    await chat.send_message(text=text, reply_markup=markup, parse_mode="HTML")


# ── Admin helpers ──────────────────────────────────────────────────────────────

async def _send(update: Update, text: str, keyboard=None, parse_mode="HTML"):
    markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode=parse_mode)
            return
        except Exception:
            chat = update.callback_query.message.chat
            await chat.send_message(text, reply_markup=markup, parse_mode=parse_mode)
    else:
        await update.message.reply_text(text, reply_markup=markup, parse_mode=parse_mode)


async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    keyboard = [
        [
            InlineKeyboardButton("📊 Статистика",    callback_data="admin_stats"),
            InlineKeyboardButton("👥 Пользователи",  callback_data="admin_users"),
        ],
        [
            InlineKeyboardButton("📢 Рассылка",      callback_data="admin_broadcast"),
            InlineKeyboardButton("📥 Экспорт CSV",   callback_data="admin_export"),
        ],
        [InlineKeyboardButton("✏️ Изменить тексты экранов",    callback_data="admin_edit_texts")],
        [InlineKeyboardButton("🖼 Изменить картинки экранов",  callback_data="admin_edit_images")],
        [InlineKeyboardButton("🏠 На главную",                 callback_data="main")],
    ]
    await _send(update, text, keyboard)


# ── Admin commands ─────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await asyncio.gather(increment_stat("starts"), register_user(user_id))
    await render_screen("main", update, context, is_new=True)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    await show_admin_panel(update, context)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = "🤖 <b>Доступные команды</b>\n\n👤 <b>Для всех:</b>\n  /start — Запустить бота\n  /help — Помощь\n"
    keyboard = []
    if is_admin(user_id):
        text += "\n🔐 <b>Администратор:</b>\n  /admin — Панель управления\n  /broadcast — Рассылка\n  /export — Экспорт CSV"
        keyboard = [[InlineKeyboardButton("🎛 Открыть панель", callback_data="admin_panel")]]
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main")])
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")


# ── Admin callbacks ────────────────────────────────────────────────────────────

async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(update.effective_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return

    data = query.data

    if data == "admin_panel":
        await show_admin_panel(update, context)

    elif data == "admin_stats":
        stats = await get_stats()
        text = (
            "📊 <b>СТАТИСТИКА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Всего пользователей: <code>{stats.get('unique_users', 0)}</code>\n"
            f"📅 Активных сегодня: <code>{stats.get('active_today', 0)}</code>\n"
            f"🎯 Всего действий: <code>{stats.get('total_actions', 0)}</code>\n\n"
            f"▶️ /start запусков: <code>{stats.get('starts', 0)}</code>\n"
            f"🛒 Покупатель: <code>{stats.get('buyer_opens', 0)}</code>\n"
            f"🏪 Продавец: <code>{stats.get('seller_opens', 0)}</code>\n"
            f"💎 ТТК: <code>{stats.get('ttk_opens', 0)}</code>\n"
            f"📄 КП открытий: <code>{stats.get('commercial_opens', 0)}</code>\n"
            f"🚀 Переходов на платформу: <code>{stats.get('platform_clicks', 0)}</code>"
        )
        await _send(update, text, [[InlineKeyboardButton("⬅️ Назад в панель", callback_data="admin_panel")]])

    elif data == "admin_users":
        users = await get_all_user_ids()
        text = f"👥 <b>Пользователи бота</b>\n\nВсего: <code>{len(users)}</code>\n\nПоследние 20:\n"
        for i, uid in enumerate(users[-20:][::-1], 1):
            text += f"{i}. <code>{uid}</code>\n"
        await _send(update, text, [[InlineKeyboardButton("⬅️ Назад в панель", callback_data="admin_panel")]])

    elif data == "admin_export":
        users = await get_all_user_ids()
        stats = await get_stats()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Метрика', 'Значение'])
        for k, label in [
            ('unique_users','Всего пользователей'), ('active_today','Активных сегодня'),
            ('total_actions','Всего действий'), ('starts','/start'),
            ('buyer_opens','Покупатель'), ('seller_opens','Продавец'),
            ('ttk_opens','ТТК'), ('platform_clicks','Платформа'),
            ('commercial_opens','КП'),
        ]:
            writer.writerow([label, stats.get(k, 0)])
        writer.writerow([])
        writer.writerow(['ID пользователей'])
        for uid in users:
            writer.writerow([uid])
        output.seek(0)
        await query.message.chat.send_document(
            document=io.BytesIO(output.getvalue().encode('utf-8')),
            filename=f"kentavr_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            caption="📊 Экспорт статистики KENTAVR MARKET",
        )


# ── Broadcast ──────────────────────────────────────────────────────────────────

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        if update.message:
            await update.message.reply_text("⛔ Доступ запрещён.")
        return ConversationHandler.END
    users = await get_all_user_ids()
    text = (
        f"📢 <b>Рассылка</b>\n\n"
        f"Аудитория: <b>{len(users)} чел.</b>\n\n"
        f"Отправь текст сообщения (HTML поддерживается).\n"
        f"/cancel — отмена"
    )
    kb = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_panel")]]
    await _send(update, text, kb)
    return BROADCAST_WAITING


async def cmd_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    users = await get_all_user_ids()
    msg = update.message.text
    sent = failed = 0
    status = await update.message.reply_text(f"⏳ Отправляю {len(users)} пользователям...")
    for i, uid in enumerate(users):
        try:
            await context.bot.send_message(uid, msg, parse_mode="HTML")
            sent += 1
        except Exception:
            failed += 1
        if (i + 1) % 30 == 0:
            await asyncio.sleep(1)
    kb = [[InlineKeyboardButton("⬅️ В панель", callback_data="admin_panel")]]
    await status.edit_text(
        f"✅ Доставлено: <b>{sent}</b>\n❌ Ошибок: <b>{failed}</b>",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def cmd_broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton("⬅️ В панель", callback_data="admin_panel")]]
    await update.message.reply_text("❌ Рассылка отменена.", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


# ── Edit texts ─────────────────────────────────────────────────────────────────

async def admin_edit_texts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton(name, callback_data=f"edit_text_{key}")]
        for key, name in SCREEN_NAMES.items()
    ]
    keyboard.append([InlineKeyboardButton("⬅️ Назад в панель", callback_data="admin_panel")])
    await _send(update, "✏️ <b>Редактирование текстов</b>\n\nВыбери экран:", keyboard)
    return EDIT_TEXT_SCREEN


async def admin_edit_text_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    screen_key = query.data[len("edit_text_"):]
    context.user_data['editing_screen'] = screen_key

    custom_text = await get_custom_text(screen_key)
    default_text, _ = SCREENS.get(screen_key, screen_main)()
    current = custom_text if custom_text else default_text
    label   = "✏️ изменён" if custom_text else "📝 оригинал"

    preview = current[:600] + ("…" if len(current) > 600 else "")
    text = (
        f"✏️ <b>{SCREEN_NAMES.get(screen_key, screen_key)}</b> <i>({label})</i>\n\n"
        f"<b>Текущий текст:</b>\n<blockquote>{preview}</blockquote>\n\n"
        f"Отправь новый текст (HTML теги поддерживаются)\nили /cancel для отмены"
    )
    keyboard = []
    if custom_text:
        keyboard.append([InlineKeyboardButton("🔄 Сбросить к оригиналу", callback_data=f"reset_text_{screen_key}")])
    keyboard.append([InlineKeyboardButton("⬅️ К списку экранов", callback_data="admin_edit_texts")])
    await _send(update, text, keyboard)
    return EDIT_TEXT_CONTENT


async def admin_save_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    screen_key = context.user_data.get('editing_screen')
    if not screen_key:
        return ConversationHandler.END
    await save_custom_text(screen_key, update.message.text)
    name = SCREEN_NAMES.get(screen_key, screen_key)
    kb = [
        [InlineKeyboardButton("✏️ Изменить ещё", callback_data="admin_edit_texts")],
        [InlineKeyboardButton("⬅️ В панель",     callback_data="admin_panel")],
    ]
    await update.message.reply_text(f"✅ Текст <b>{name}</b> обновлён!", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END


async def admin_reset_text_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    screen_key = query.data[len("reset_text_"):]
    await reset_custom_text(screen_key)
    name = SCREEN_NAMES.get(screen_key, screen_key)
    kb = [[InlineKeyboardButton("⬅️ В панель", callback_data="admin_panel")]]
    await _send(update, f"🔄 Текст <b>{name}</b> сброшен к оригиналу.", kb)
    return ConversationHandler.END


async def admin_edit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop('editing_screen', None)
    kb = [[InlineKeyboardButton("⬅️ В панель", callback_data="admin_panel")]]
    await update.message.reply_text("❌ Редактирование отменено.", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


async def admin_panel_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    context.user_data.pop('editing_screen', None)
    await show_admin_panel(update, context)
    return ConversationHandler.END


# ── Edit images ────────────────────────────────────────────────────────────────

async def admin_edit_images_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton(SCREEN_NAMES[k], callback_data=f"edit_image_{k}")]
        for k in ["main", "buyer", "seller", "ttk"]
    ]
    keyboard.append([InlineKeyboardButton("⬅️ Назад в панель", callback_data="admin_panel")])
    await _send(update, "🖼 <b>Редактирование картинок</b>\n\nВыбери экран:", keyboard)
    return EDIT_IMAGE_SCREEN


async def admin_edit_image_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    screen_key = query.data[len("edit_image_"):]
    context.user_data['editing_screen'] = screen_key

    custom_image = await get_custom_image(screen_key)
    current = custom_image if custom_image else SCREEN_IMAGES.get(screen_key, "нет")
    label   = "✏️ изменена" if custom_image else "📝 оригинал"

    text = (
        f"🖼 <b>{SCREEN_NAMES.get(screen_key, screen_key)}</b> <i>({label})</i>\n\n"
        f"<b>Текущая картинка:</b>\n<code>{current}</code>\n\n"
        f"Отправь новую ссылку на картинку (https://…)\nили /cancel для отмены"
    )
    keyboard = []
    if custom_image:
        keyboard.append([InlineKeyboardButton("🔄 Сбросить к оригиналу", callback_data=f"reset_image_{screen_key}")])
    keyboard.append([InlineKeyboardButton("⬅️ К списку экранов", callback_data="admin_edit_images")])
    await _send(update, text, keyboard)
    return EDIT_IMAGE_URL


async def admin_save_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    screen_key = context.user_data.get('editing_screen')
    if not screen_key:
        return ConversationHandler.END
    url = update.message.text.strip()
    if not url.startswith("http"):
        await update.message.reply_text("❌ Ссылка должна начинаться с https://. Попробуй ещё раз:")
        return EDIT_IMAGE_URL
    await save_custom_image(screen_key, url)
    name = SCREEN_NAMES.get(screen_key, screen_key)
    kb = [
        [InlineKeyboardButton("🖼 Изменить ещё", callback_data="admin_edit_images")],
        [InlineKeyboardButton("⬅️ В панель",     callback_data="admin_panel")],
    ]
    await update.message.reply_text(f"✅ Картинка <b>{name}</b> обновлена!", reply_markup=InlineKeyboardMarkup(kb), parse_mode="HTML")
    return ConversationHandler.END


async def admin_reset_image_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    screen_key = query.data[len("reset_image_"):]
    await reset_custom_image(screen_key)
    name = SCREEN_NAMES.get(screen_key, screen_key)
    kb = [[InlineKeyboardButton("⬅️ В панель", callback_data="admin_panel")]]
    await _send(update, f"🔄 Картинка <b>{name}</b> сброшена к оригиналу.", kb)
    return ConversationHandler.END


# ── Misc handlers ──────────────────────────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await render_screen(query.data, update, context)


async def web_app_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await increment_stat("commercial_opens")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Unhandled error: {context.error}")


# ── Main ───────────────────────────────────────────────────────────────────────

async def async_main():
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help",  cmd_help))
    app.add_handler(CommandHandler("admin", cmd_admin))

    # Broadcast conversation
    app.add_handler(ConversationHandler(
        entry_points=[
            CommandHandler("broadcast", cmd_broadcast),
            CallbackQueryHandler(cmd_broadcast, pattern="^admin_broadcast$"),
        ],
        states={
            BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_broadcast_send)],
        },
        fallbacks=[
            CommandHandler("cancel", cmd_broadcast_cancel),
            CallbackQueryHandler(admin_panel_fallback, pattern="^admin_panel$"),
        ],
    ))

    # Edit content conversation
    app.add_handler(ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_edit_texts_menu,  pattern="^admin_edit_texts$"),
            CallbackQueryHandler(admin_edit_images_menu, pattern="^admin_edit_images$"),
        ],
        states={
            EDIT_TEXT_SCREEN: [
                CallbackQueryHandler(admin_edit_text_select, pattern="^edit_text_"),
                CallbackQueryHandler(admin_panel_fallback,   pattern="^admin_panel$"),
            ],
            EDIT_TEXT_CONTENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_save_text),
                CallbackQueryHandler(admin_reset_text_cb,    pattern="^reset_text_"),
                CallbackQueryHandler(admin_edit_texts_menu,  pattern="^admin_edit_texts$"),
                CallbackQueryHandler(admin_panel_fallback,   pattern="^admin_panel$"),
            ],
            EDIT_IMAGE_SCREEN: [
                CallbackQueryHandler(admin_edit_image_select, pattern="^edit_image_"),
                CallbackQueryHandler(admin_panel_fallback,    pattern="^admin_panel$"),
            ],
            EDIT_IMAGE_URL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_save_image),
                CallbackQueryHandler(admin_reset_image_cb,    pattern="^reset_image_"),
                CallbackQueryHandler(admin_edit_images_menu,  pattern="^admin_edit_images$"),
                CallbackQueryHandler(admin_panel_fallback,    pattern="^admin_panel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", admin_edit_cancel),
            CallbackQueryHandler(admin_panel_fallback, pattern="^admin_panel$"),
        ],
    ))

    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_handler))

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
            headers={'Content-Type': 'application/json'},
        )
        urllib.request.urlopen(req, timeout=5)
        logger.info("Webhook очищен")
    except Exception:
        pass
    threading.Thread(target=_start_health_server, daemon=True).start()
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
