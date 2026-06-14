import os
import logging
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta

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

# Пытаемся импортировать PostgreSQL, если нет - используем SQLite
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    import aiosqlite

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PLATFORM_URL = os.getenv("PLATFORM_URL", "https://kentavr.world/?ref=kentavrmarket").strip()
LANDING_URL = os.getenv("LANDING_URL", "https://kentavr.world").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

DB_PATH = "kentavr_stats.db"
BROADCAST_WAITING = 1

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Глобальная переменная для подключения к БД
db_pool = None


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
# DATABASE (PostgreSQL или SQLite)
# ─────────────────────────────────────────────

async def init_db():
    global db_pool
    
    if DATABASE_URL and POSTGRES_AVAILABLE:
        # Используем PostgreSQL
        logger.info("Using PostgreSQL database")
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
                    actions_count INTEGER DEFAULT 0,
                    referral_code TEXT,
                    referred_by BIGINT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id BIGINT PRIMARY KEY,
                    last_screen TEXT DEFAULT 'main',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    code TEXT PRIMARY KEY,
                    owner_id BIGINT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            keys = ("starts", "buyer_opens", "seller_opens", "ttk_opens", 
                    "platform_clicks", "commercial_opens", "webapp_opens")
            for key in keys:
                await conn.execute(
                    "INSERT INTO stats (key, value) VALUES ($1, 0) ON CONFLICT (key) DO NOTHING",
                    key
                )
        logger.info("PostgreSQL initialized successfully")
    else:
        # Используем SQLite (fallback)
        logger.info("Using SQLite database")
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
                    actions_count INTEGER DEFAULT 0,
                    referral_code TEXT,
                    referred_by INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id INTEGER PRIMARY KEY,
                    last_screen TEXT DEFAULT 'main',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    code TEXT PRIMARY KEY,
                    owner_id INTEGER UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            keys = ("starts", "buyer_opens", "seller_opens", "ttk_opens", 
                    "platform_clicks", "commercial_opens", "webapp_opens")
            for key in keys:
                await db.execute(
                    "INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,)
                )
            await db.commit()
        logger.info("SQLite initialized successfully")


async def increment_stat(key: str):
    if db_pool and DATABASE_URL:
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


async def register_user(user_id: int, referral_code: str = None):
    if db_pool and DATABASE_URL:
        async with db_pool.acquire() as conn:
            # Проверяем, существует ли пользователь
            existing = await conn.fetchval("SELECT user_id FROM unique_users WHERE user_id = $1", user_id)
            if existing:
                await conn.execute(
                    "UPDATE unique_users SET last_seen = CURRENT_TIMESTAMP, actions_count = actions_count + 1 WHERE user_id = $1",
                    user_id
                )
            else:
                # Генерируем реферальный код для нового пользователя
                import random
                import string
                ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                await conn.execute("""
                    INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count, referral_code)
                    VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, $2)
                """, user_id, ref_code)
                await conn.execute(
                    "INSERT INTO referrals (code, owner_id) VALUES ($1, $2)",
                    ref_code, user_id
                )
                
                # Если есть реферальный код от другого пользователя
                if referral_code:
                    referrer = await conn.fetchval("SELECT owner_id FROM referrals WHERE code = $1", referral_code)
                    if referrer:
                        await conn.execute(
                            "UPDATE unique_users SET referred_by = $1 WHERE user_id = $2",
                            referrer, user_id
                        )
                        # Начисляем бонус рефереру
                        await increment_stat("referrals_count")
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
                import random
                import string
                ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                await db.execute("""
                    INSERT INTO unique_users (user_id, first_seen, last_seen, actions_count, referral_code)
                    VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, ?)
                """, (user_id, ref_code))
                await db.execute(
                    "INSERT INTO referrals (code, owner_id) VALUES (?, ?)",
                    (ref_code, user_id)
                )
                
                if referral_code:
                    cursor = await db.execute("SELECT owner_id FROM referrals WHERE code = ?", (referral_code,))
                    row = await cursor.fetchone()
                    if row:
                        await db.execute(
                            "UPDATE unique_users SET referred_by = ? WHERE user_id = ?",
                            (row[0], user_id)
                        )
                        await increment_stat("referrals_count")
            await db.commit()


async def save_user_session(user_id: int, screen: str):
    if db_pool and DATABASE_URL:
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


async def get_user_session(user_id: int) -> str:
    if db_pool and DATABASE_URL:
        async with db_pool.acquire() as conn:
            screen = await conn.fetchval(
                "SELECT last_screen FROM user_sessions WHERE user_id = $1", user_id
            )
            return screen or "main"
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT last_screen FROM user_sessions WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row else "main"


async def get_user_referral_code(user_id: int) -> str:
    if db_pool and DATABASE_URL:
        async with db_pool.acquire() as conn:
            code = await conn.fetchval(
                "SELECT referral_code FROM unique_users WHERE user_id = $1", user_id
            )
            return code
    else:
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT referral_code FROM unique_users WHERE user_id = ?", (user_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_stats() -> dict:
    if db_pool and DATABASE_URL:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM stats")
            stats = {row['key']: row['value'] for row in rows}
            users_count = await conn.fetchval("SELECT COUNT(*) FROM unique_users")
            stats["unique_users"] = users_count if users_count else 0
            
            # Дополнительная статистика
            today = datetime.now().date()
            active_today = await conn.fetchval(
                "SELECT COUNT(*) FROM unique_users WHERE DATE(last_seen) = CURRENT_DATE"
            )
            stats["active_today"] = active_today if active_today else 0
            
            total_actions = await conn.fetchval("SELECT SUM(actions_count) FROM unique_users")
            stats["total_actions"] = total_actions if total_actions else 0
            
            referrals = await conn.fetchval("SELECT COUNT(*) FROM unique_users WHERE referred_by IS NOT NULL")
            stats["referrals"] = referrals if referrals else 0
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
            
            cursor5 = await db.execute("SELECT COUNT(*) FROM unique_users WHERE referred_by IS NOT NULL")
            row5 = await cursor5.fetchone()
            stats["referrals"] = row5[0] if row5 else 0
    return stats


async def get_all_user_ids() -> list[int]:
    if db_pool and DATABASE_URL:
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
# SCREEN DEFINITIONS (полные версии всех экранов)
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
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_seller_detail():
    text = (
        "<b>📖 Подробнее</b>\n\n"
        "Ключевая идея <b>KENTAVR MARKET</b> — объединение покупателей и продавцов "
        "внутри единой торговой среды.\n\n"
        "<blockquote>Чем активнее развивается 