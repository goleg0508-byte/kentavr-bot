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
    InlineQueryHandler,
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

# ========== КАРТИНКИ ДЛЯ ЭКРАНОВ ==========
SCREEN_IMAGES = {
    "main": "https://i.postimg.cc/RFsmw06x/Chat-GPT-Image-4-iun-2026-g-06-12-26.png",
    "buyer": "https://i.postimg.cc/wTSw7dBP/Chat-GPT-Image-4-iun-2026-g-06-35-43.png",
    "seller": "https://i.postimg.cc/TwFDCfFH/IMG-20260604-035610-329.png",
    "ttk": "https://i.postimg.cc/fT5gqd27/Chat-GPT-Image-4-iun-2026-g-03-57-21.png",
}
# ==========================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

db_pool = None
USE_POSTGRES = False


# ─────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass


def _start_health_server():
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    logger.info("Health server started on port %s", port)
    server.serve_forever()


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

async def init_db():
    global db_pool, USE_POSTGRES
    
    if DATABASE_URL:
        try:
            import asyncpg
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        key TEXT PRIMARY KEY,
                        value INTEGER DEFAULT 0
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS unique_users (
                        user_id BIGINT PRIMARY KEY,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        actions_count INTEGER DEFAULT 0
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_sessions (
                        user_id BIGINT PRIMARY KEY,
                        last_screen TEXT DEFAULT 'main',
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                keys = ("starts", "buyer_opens", "seller_opens", "ttk_opens", 
                        "platform_clicks", "commercial_opens")
                for key in keys:
                    await conn.execute(
                        "INSERT INTO stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING",
                        key
                    )
            USE_POSTGRES = True
            logger.info("✅ PostgreSQL initialized successfully")
            return
        except Exception as e:
            logger.warning(f"PostgreSQL connection failed: {e}, falling back to SQLite")
    
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS unique_users (
                user_id INTEGER PRIMARY KEY,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actions_count INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id INTEGER PRIMARY KEY,
                last_screen TEXT DEFAULT 'main',
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        keys = ("starts", "buyer_opens", "seller_opens", "ttk_opens", 
                "platform_clicks", "commercial_opens")
        for key in keys:
            await db.execute(
                "INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,)
            )
        await db.commit()
    USE_POSTGRES = False
    logger.info("✅ SQLite initialized successfully")


async def increment_stat(key: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO stats (key, value) VALUES ($1, 1) "
                "ON CONFLICT (key) DO UPDATE SET value = stats.value + 1",
                key
            )
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO stats (key, value) VALUES (?, 1) "
                "ON CONFLICT(key) DO UPDATE SET value = value + 1",
                (key,),
            )
            await db.commit()


async def register_user(user_id: int):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            existing = await conn.fetchval("SELECT user_id FROM unique_users WHERE user_id = $1", user_id)
            if existing:
                await conn.execute(
                    "UPDATE unique_users SET last_seen = CURRENT_TIMESTAMP, actions_count = actions_count + 1 WHERE user_id = $1",
                    user_id
                )
            else:
                await conn.execute("""
                    INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count)
                    VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """, user_id)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_id FROM unique_users WHERE user_id = ?", (user_id,))
            existing = await cursor.fetchone()
            if existing:
                await db.execute(
                    "UPDATE unique_users SET last_seen = CURRENT_TIMESTAMP, actions_count = actions_count + 1 WHERE user_id = ?",
                    (user_id,)
                )
            else:
                await db.execute("""
                    INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count)
                    VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                """, (user_id,))
            await db.commit()


async def save_user_session(user_id: int, screen: str):
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO user_sessions (user_id, last_screen, last_updated)
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET 
                    last_screen = $2, last_updated = CURRENT_TIMESTAMP
            """, user_id, screen)
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO user_sessions (user_id, last_screen, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET 
                    last_screen = ?, last_updated = CURRENT_TIMESTAMP
            """, (user_id, screen, screen))
            await db.commit()


async def get_stats() -> dict:
    stats = {}
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM stats")
            stats = {row['key']: row['value'] for row in rows}
            users_count = await conn.fetchval("SELECT COUNT(*) FROM unique_users")
            stats["unique_users"] = users_count if users_count else 0
            
            active_today = await conn.fetchval(
                "SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen) = CURRENT_DATE"
            )
            stats["active_today"] = active_today if active_today else 0
            
            total_actions = await conn.fetchval("SELECT SUM(actions_count) FROM unique_users")
            stats["total_actions"] = total_actions if total_actions else 0
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT key, value FROM stats")
            rows = await cursor.fetchall()
            stats = {row[0]: row[1] for row in rows}
            cursor2 = await db.execute("SELECT COUNT(*) FROM unique_users")
            row2 = await cursor2.fetchone()
            stats["unique_users"] = row2[0] if row2 else 0
            
            cursor3 = await db.execute("SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen) = DATE('now')")
            row3 = await cursor3.fetchone()
            stats["active_today"] = row3[0] if row3 else 0
            
            cursor4 = await db.execute("SELECT SUM(actions_count) FROM unique_users")
            row4 = await cursor4.fetchone()
            stats["total_actions"] = row4[0] if row4 else 0
    return stats


async def get_all_user_ids() -> list[int]:
    if USE_POSTGRES and db_pool:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM unique_users")
            return [row['user_id'] for row in rows]
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_id FROM unique_users")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ─────────────────────────────────────────────
# SCREEN DEFINITIONS
# ─────────────────────────────────────────────

def screen_main():
    text = (
        "<b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b>.\n\n"
        "Это социальный маркетплейс нового поколения, объединяющий покупателей, "
        "продавцов и партнёров в единую систему взаимной выгоды.\n\n"
        "<blockquote>Основой модели является <b>Торговый Токен KENTAVR (ТТК)</b> — внутренний "
        "цифровой инструмент, применяемый для бонусов, cashback и участия в развитии "
        "сообщества.</blockquote>\n\n"
        "Здесь каждый активный участник может использовать возможности торговой среды "
        "не только для покупок или продаж, но и для участия в развитии общей системы.\n\n"
        "<i>Что тебе сейчас ближе?</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🛒 Я покупатель", callback_data="buyer")],
        [InlineKeyboardButton("🏪 Я продавец", callback_data="seller")],
        [InlineKeyboardButton("💎 Хочу узнать про ТТК", callback_data="ttk")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("🚀 Перейти на платформу", callback_data="goto_platform")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_buyer():
    text = (
        "<b>🛒 Для покупателя</b>\n\n"
        "На большинстве торговых площадок всё заканчивается покупкой.\n\n"
        "В <b>KENTAVR MARKET</b> подход иной.\n\n"
        "<blockquote>Совершая покупки внутри сообщества, ты становишься частью среды, где "
        "активность участников формирует общий товарооборот.</blockquote>\n\n"
        "Дополнительно могут начисляться бонусы в виде <b>Торгового Токена KENTAVR (ТТК)</b>, "
        "применяемого для различных возможностей и расчётов внутри системы.\n\n"
        "Привычная покупка превращается в элемент более широкой модели взаимодействия.\n\n"
        "<i>Оценить идею проще всего изнутри.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📖 Узнать подробнее", callback_data="buyer_detail")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_buyer_detail():
    text = (
        "<b>📖 Подробнее</b>\n\n"
        "<b>KENTAVR MARKET</b> позиционируется как первый социальный маркетплейс, "
        "где равное внимание уделяется и покупателям, и продавцам.\n\n"
        "<blockquote>Модель выстроена вокруг идеи взаимной выгоды и развития внутреннего "
        "товарооборота.</blockquote>\n\n"
        "Приобретая товары, услуги или интеллектуальные продукты у участников "
        "сообщества, ты одновременно поддерживаешь деловую среду, частью которой "
        "сам являешься.\n\n"
        "<i>Посмотри, как это работает на практике.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="buyer")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_seller():
    text = (
        "<b>🏪 Для продавца</b>\n\n"
        "Если ты предлагаешь товары, услуги или интеллектуальные продукты, "
        "<b>KENTAVR MARKET</b> — это не просто торговая витрина.\n\n"
        "<blockquote>Ты получаешь доступ к сообществу активных людей, заинтересованных в "
        "развитии внутреннего оборота и долгосрочном сотрудничестве.</blockquote>\n\n"
        "В отличие от классических площадок, акцент здесь делается не только на "
        "продажах, но и на формировании устойчивых деловых связей между участниками.\n\n"
        "<i>Следующий шаг — познакомиться с возможностями системы.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📖 Узнать подробнее", callback_data="seller_detail")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_seller_detail():
    text = (
        "<b>📖 Подробнее</b>\n\n"
        "Ключевая идея <b>KENTAVR MARKET</b> — объединение покупателей и продавцов "
        "внутри единой торговой среды.\n\n"
        "<blockquote>Чем активнее развивается товарооборот, тем больше возможностей появляется "
        "у участников: для продвижения предложений, расширения клиентской базы и "
        "выстраивания партнёрских связей.</blockquote>\n\n"
        "Модель ориентирована на формирование долгосрочных отношений, а не "
        "разовых сделок.\n\n"
        "<i>Посмотри, как это устроено изнутри.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="seller")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk():
    text = (
        "<b>💎 Торговый Токен KENTAVR (ТТК)</b>\n\n"
        "<b>ТТК</b> — это внутренний цифровой инструмент, действующий в рамках экосистемы "
        "<b>KENTAVR MARKET</b>.\n\n"
        "<blockquote>Применяется для начисления бонусов, cashback, частичной оплаты товаров "
        "и услуг, а также как элемент участия в развитии сообщества.</blockquote>\n\n"
        "<b>ТТК</b> — важная часть бизнес-модели платформы, связанная с внутренними "
        "процессами торговой среды.\n\n"
        "<i>Что тебя интересует?</i>"
    )
    keyboard = [
        [InlineKeyboardButton("❓ Это криптовалюта?", callback_data="ttk_crypto")],
        [InlineKeyboardButton("💰 В чём выгода?", callback_data="ttk_benefit")],
        [InlineKeyboardButton("⭐ Почему это уникально?", callback_data="ttk_unique")],
        [InlineKeyboardButton("🚀 Как начать?", callback_data="ttk_start")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_crypto():
    text = (
        "<b>❓ Это криптовалюта?</b>\n\n"
        "<b>ТТК</b> не является классической биржевой криптовалютой.\n\n"
        "<blockquote>Это внутренний торговый токен, применяемый в системе <b>KENTAVR MARKET</b> "
        "для бонусов, cashback и операций между участниками сообщества.</blockquote>\n\n"
        "<i>Его ценность определяется активностью и товарооборотом внутри экосистемы, "
        "а не биржевыми котировками.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="ttk")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_benefit():
    text = (
        "<b>💰 В чём выгода?</b>\n\n"
        "Главная идея — объединение покупательской и предпринимательской активности "
        "в единой среде.\n\n"
        "<blockquote>Чем больше взаимодействий происходит внутри сообщества, тем активнее "
        "развивается общий товарооборот и расширяются возможности для каждого "
        "участника.</blockquote>\n\n"
        "<i>ТТК служит связующим инструментом, который делает участие в системе "
        "более предметным.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="ttk")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_unique():
    text = (
        "<b>⭐ Почему это уникально?</b>\n\n"
        "<b>KENTAVR MARKET</b> сочетает возможности маркетплейса, делового сообщества "
        "и токенизированной модели взаимодействия.\n\n"
        "<blockquote>Такой подход формирует среду, где покупатели, продавцы и партнёры объединены "
        "общей системой сотрудничества и внутреннего обмена ценностью.</blockquote>\n\n"
        "<i>Это выходит за рамки привычного формата торговой площадки.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="ttk")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_start():
    text = (
        "<b>🚀 Как начать?</b>\n\n"
        "<blockquote>Лучший способ разобраться в возможностях <b>KENTAVR MARKET</b> — "
        "изучить систему изнутри.</blockquote>\n\n"
        "<i>Перейди на платформу и выбери направление, которое интересно именно тебе: "
        "покупки, продажи или партнёрство через ТТК.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на платформу", callback_data="goto_platform")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_platform():
    text = (
        "<b>Отлично!</b>\n\n"
        "Сейчас откроется <b>KENTAVR MARKET</b>.\n\n"
        "<blockquote>Познакомься с возможностями сообщества, изучи предложения участников "
        "и выбери направление, которое подходит именно тебе.</blockquote>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Открыть KENTAVR MARKET", url=PLATFORM_URL)],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


SCREENS = {
    "main": screen_main,
    "buyer": screen_buyer,
    "buyer_detail": screen_buyer_detail,
    "seller": screen_seller,
    "seller_detail": screen_seller_detail,
    "ttk": screen_ttk,
    "ttk_crypto": screen_ttk_crypto,
    "ttk_benefit": screen_ttk_benefit,
    "ttk_unique": screen_ttk_unique,
    "ttk_start": screen_ttk_start,
    "goto_platform": screen_platform,
}

STAT_MAP = {
    "buyer": "buyer_opens",
    "seller": "seller_opens",
    "ttk": "ttk_opens",
    "goto_platform": "platform_clicks",
}


# ─────────────────────────────────────────────
# RENDER ENGINE (с поддержкой картинок)
# ─────────────────────────────────────────────

async def render_screen(
    screen_key: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    is_new: bool = False,
):
    user_id = update.effective_user.id
    await save_user_session(user_id, screen_key)
    
    builder = SCREENS.get(screen_key, screen_main)
    text, markup = builder()

    if screen_key in STAT_MAP:
        await increment_stat(STAT_MAP[screen_key])

    if is_new:
        image_url = SCREEN_IMAGES.get(screen_key)
        if image_url:
            await update.message.reply_photo(
                photo=image_url,
                caption=text,
                reply_markup=markup,
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
        return

    query = update.callback_query
    try:
        # При редактировании сообщения картинку не меняем (только текст)
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


# ─────────────────────────────────────────────
# ADMIN COMMANDS
# ─────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список доступных команд"""
    user_id = update.effective_user.id
    is_admin_user = is_admin(user_id)
    
    text = "🤖 <b>Доступные команды</b>\n\n"
    text += "👤 <b>Для всех пользователей:</b>\n"
    text += "  /start - Запустить бота\n"
    text += "  /help  - Показать это сообщение\n\n"
    
    if is_admin_user:
        text += "🔐 <b>Команды администратора:</b>\n"
        text += "  /admin     - Краткая статистика\n"
        text += "  /stats     - Детальная статистика\n"
        text += "  /users     - Список пользователей\n"
        text += "  /export    - Экспорт данных (CSV)\n"
        text += "  /broadcast - Сделать рассылку\n"
        text += "  /help      - Это сообщение\n\n"
    
    text += "<i>Бот автоматически обновляется, все команды доступны сразу после деплоя.</i>"
    
    if is_admin_user:
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton("📥 Экспорт CSV", callback_data="admin_export")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    else:
        await update.message.reply_text(text, parse_mode="HTML")


async def cmd_stats_detailed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детальная статистика для админа"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    stats = await get_stats()
    
    text = (
        "📊 <b>ДЕТАЛЬНАЯ СТАТИСТИКА</b>\n"
        "╔════════════════════════════════╗\n\n"
        
        "👥 <b>Пользователи:</b>\n"
        f"   • Всего: <code>{stats.get('unique_users', 0)}</code>\n"
        f"   • Активных сегодня: <code>{stats.get('active_today', 0)}</code>\n"
        f"   • Всего действий: <code>{stats.get('total_actions', 0)}</code>\n\n"
        
        "📈 <b>Активность:</b>\n"
        f"   • /start: <code>{stats.get('starts', 0)}</code>\n"
        f"   • Покупатель: <code>{stats.get('buyer_opens', 0)}</code>\n"
        f"   • Продавец: <code>{stats.get('seller_opens', 0)}</code>\n"
        f"   • ТТК: <code>{stats.get('ttk_opens', 0)}</code>\n"
        f"   • Платформа: <code>{stats.get('platform_clicks', 0)}</code>\n\n"
        
        "📄 <b>Коммерческое предложение:</b>\n"
        f"   • Открытий: <code>{stats.get('commercial_opens', 0)}</code>\n\n"
        
        "╚════════════════════════════════╝\n"
        "<i>Данные обновляются в реальном времени</i>"
    )
    
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список последних пользователей"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    user_ids = await get_all_user_ids()
    total_users = len(user_ids)
    
    if total_users == 0:
        await update.message.reply_text("📭 Пока нет пользователей.")
        return
    
    last_20 = user_ids[-20:][::-1]
    
    text = f"👥 <b>Пользователи бота</b>\n\n"
    text += f"📊 Всего: <code>{total_users}</code>\n"
    text += f"📋 Последние 20:\n\n"
    
    for i, uid in enumerate(last_20, 1):
        text += f"   {i}. <code>{uid}</code>\n"
    
    text += f"\n<i>Полный список можно экспортировать через /export</i>"
    
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт данных в CSV"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    status_msg = await update.message.reply_text("⏳ Формирую отчёт...")
    
    stats = await get_stats()
    user_ids = await get_all_user_ids()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Метрика', 'Значение'])
    writer.writerow(['Всего пользователей', stats.get('unique_users', 0)])
    writer.writerow(['Активных сегодня', stats.get('active_today', 0)])
    writer.writerow(['Всего действий', stats.get('total_actions', 0)])
    writer.writerow(['/start', stats.get('starts', 0)])
    writer.writerow(['Покупатель', stats.get('buyer_opens', 0)])
    writer.writerow(['Продавец', stats.get('seller_opens', 0)])
    writer.writerow(['ТТК', stats.get('ttk_opens', 0)])
    writer.writerow(['Переходов на платформу', stats.get('platform_clicks', 0)])
    writer.writerow(['Открытий КП', stats.get('commercial_opens', 0)])
    writer.writerow([])
    writer.writerow(['ID пользователей'])
    for uid in user_ids:
        writer.writerow([uid])
    
    output.seek(0)
    await status_msg.delete()
    
    await update.message.reply_document(
        document=io.BytesIO(output.getvalue().encode('utf-8')),
        filename=f"kentavr_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        caption="📊 Экспорт статистики KENTAVR MARKET"
    )


# ─────────────────────────────────────────────
# HANDLERS
# ─────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await asyncio.gather(
        increment_stat("starts"),
        register_user(user_id),
    )
    await render_screen("main", update, context, is_new=True)
    return ConversationHandler.END


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    stats = await get_stats()
    text = (
        "📊 <b>Статистика KENTAVR MARKET Bot</b>\n\n"
        f"👥 Уникальных пользователей: <b>{stats.get('unique_users', 0)}</b>\n"
        f"📅 Активных сегодня: <b>{stats.get('active_today', 0)}</b>\n"
        f"🎯 Всего действий: <b>{stats.get('total_actions', 0)}</b>\n\n"
        f"▶️ Запусков /start: <b>{stats.get('starts', 0)}</b>\n\n"
        f"🛒 Открытий раздела покупателя: <b>{stats.get('buyer_opens', 0)}</b>\n"
        f"🏪 Открытий раздела продавца: <b>{stats.get('seller_opens', 0)}</b>\n"
        f"💎 Открытий раздела ТТК: <b>{stats.get('ttk_opens', 0)}</b>\n"
        f"📄 Открытий коммерческого предложения: <b>{stats.get('commercial_opens', 0)}</b>\n\n"
        f"🚀 Переходов на платформу: <b>{stats.get('platform_clicks', 0)}</b>\n\n"
        "📣 Для рассылки используй /broadcast"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await render_screen(query.data, update, context)


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик inline-кнопок админ-панели"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await query.edit_message_text("⛔ Доступ запрещён.")
        return
    
    data = query.data
    
    if data == "admin_stats":
        await cmd_stats_detailed(update, context)
    elif data == "admin_broadcast":
        await cmd_broadcast_start(update, context)
    elif data == "admin_users":
        await cmd_users_list(update, context)
    elif data == "admin_export":
        await cmd_export_data(update, context)
    elif data == "main":
        await render_screen("main", update, context)
    else:
        await render_screen(data, update, context)


async def cmd_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    user_ids = await get_all_user_ids()
    await update.message.reply_text(
        f"📣 <b>Рассылка</b>\n\n"
        f"Аудитория: <b>{len(user_ids)}</b> пользователей.\n\n"
        "Напиши текст сообщения — поддерживается HTML-разметка.\n\n"
        "<i>Для отмены отправь /cancel</i>",
        parse_mode="HTML",
    )
    return BROADCAST_WAITING


async def cmd_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return ConversationHandler.END
    
    user_ids = await get_all_user_ids()
    message_text = update.message.text
    
    status_msg = await update.message.reply_text(
        f"⏳ Отправляю сообщение {len(user_ids)} пользователям..."
    )
    
    sent, failed = 0, 0
    for i, uid in enumerate(user_ids):
        try:
            await context.bot.send_message(chat_id=uid, text=message_text, parse_mode="HTML")
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            logger.warning("Broadcast error for user %d: %s", uid, e)
            failed += 1
        
        if (i + 1) % 30 == 0:
            await asyncio.sleep(1)
    
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена</b>\n\n"
        f"📨 Доставлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{failed}</b>",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def cmd_broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отменено.")
    return ConversationHandler.END


async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InlineQueryResultArticle, InputTextMessageContent
    
    results = [
        InlineQueryResultArticle(
            id="1",
            title="KENTAVR MARKET - Социальный маркетплейс",
            description="Узнай больше о платформе",
            input_message_content=InputTextMessageContent(
                f"🚀 <b>KENTAVR MARKET</b>\n\n{PLATFORM_URL}",
                parse_mode="HTML"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Открыть", url=PLATFORM_URL)],
            ])
        ),
    ]
    await update.inline_query.answer(results, cache_time=300)


async def web_app_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await increment_stat("commercial_opens")
    await update.message.reply_text("✅ Спасибо за интерес к коммерческому предложению!")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

async def async_main():
    await init_db()
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)

    broadcast_handler = ConversationHandler(
        entry_points=[CommandHandler("broadcast", cmd_broadcast_start)],
        states={
            BROADCAST_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_broadcast_send)
            ]
        },
        fallbacks=[CommandHandler("cancel", cmd_broadcast_cancel)],
    )

    # Основные команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("stats", cmd_stats_detailed))
    app.add_handler(CommandHandler("users", cmd_users_list))
    app.add_handler(CommandHandler("export", cmd_export_data))
    app.add_handler(broadcast_handler)
    
    # Обработчики кнопок
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # Дополнительные обработчики
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_handler))
    app.add_handler(InlineQueryHandler(inline_query_handler))

    logger.info("Bot started successfully!")
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    
    if not ADMIN_IDS:
        logger.warning("⚠️ ADMIN_IDS not set! /admin command available to everyone!")
    
    # Очистка вебхука
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        data = json.dumps({"drop_pending_updates": True}).encode()
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=5)
        logger.info("Webhook cleared")
    except Exception as e:
        logger.warning(f"Webhook clear failed: {e}")

    threading.Thread(target=_start_health_server, daemon=True).start()
    
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")


if __name__ == "__main__":
    main()