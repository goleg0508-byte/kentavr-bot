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
        "<blockquote>Чем активнее развивается товарооборот, тем больше возможностей появляется "
        "у участников: для продвижения предложений, расширения клиентской базы и "
        "выстраивания партнёрских связей.</blockquote>\n\n"
        "Модель ориентирована на формирование долгосрочных отношений, а не "
        "разовых сделок.\n\n"
        "<i>Посмотри, как это устроено изнутри.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
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
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
        [InlineKeyboardButton("👥 Пригласить друга", callback_data="referral")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_referral(user_id: int):
    import asyncio
    # Получаем реферальный код (синхронно в асинхронном контексте - нужно переделать)
    # Временно используем заглушку
    text = (
        "<b>👥 Пригласи друга в KENTAVR MARKET!</b>\n\n"
        "Поделись ссылкой с друзьями и получай бонусы за каждого приглашённого!\n\n"
        "<blockquote>Твоя реферальная ссылка:\n"
        f"<code>https://t.me/{(os.getenv('BOT_USERNAME', 'kentavr_bot'))}?start=ref_ТВОЙ_КОД</code></blockquote>\n\n"
        "<i>Скопируй ссылку и отправь другу. Когда он зарегистрируется, вы оба получите бонусы!</i>"
    )
    keyboard = [
        [InlineKeyboardButton("📤 Поделиться ссылкой", switch_inline_query="Присоединяйся к KENTAVR MARKET!")],
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
    "referral": screen_referral,
}

STAT_MAP = {
    "buyer": "buyer_opens",
    "seller": "seller_opens",
    "ttk": "ttk_opens",
    "goto_platform": "platform_clicks",
}


# ─────────────────────────────────────────────
# RENDER ENGINE
# ─────────────────────────────────────────────

async def render_screen(
    screen_key: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    is_new: bool = False,
):
    user_id = update.effective_user.id
    
    # Сохраняем сессию пользователя
    await save_user_session(user_id, screen_key)
    
    # Получаем функцию отрисовки экрана
    builder = SCREENS.get(screen_key, screen_main)
    
    # Если экран referral, передаём user_id
    if screen_key == "referral":
        text, markup = builder(user_id)
    else:
        text, markup = builder()

    if screen_key in STAT_MAP:
        await increment_stat(STAT_MAP[screen_key])
    
    if screen_key == "commercial" or screen_key == "webapp":
        await increment_stat("commercial_opens")

    if is_new:
        await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
        return

    query = update.callback_query
    try:
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


# ─────────────────────────────────────────────
# HANDLERS — MAIN BOT
# ─────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Проверяем реферальный код
    referral_code = None
    if context.args and context.args[0].startswith("ref_"):
        referral_code = context.args[0][4:]
    
    await asyncio.gather(
        increment_stat("starts"),
        register_user(user_id, referral_code),
    )
    await render_screen("main", update, context, is_new=True)
    return ConversationHandler.END


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Защита админ-команды
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён. Эта команда только для администраторов.")
        return
    
    stats = await get_stats()
    text = (
        "📊 <b>Статистика KENTAVR MARKET Bot</b>\n\n"
        f"👥 Уникальных пользователей: <b>{stats.get('unique_users', 0)}</b>\n"
        f"📅 Активных сегодня: <b>{stats.get('active_today', 0)}</b>\n"
        f"🎯 Всего действий: <b>{stats.get('total_actions', 0)}</b>\n"
        f"👥 Пришло по рефералке: <b>{stats.get('referrals', 0)}</b>\n\n"
        f"▶️ Запусков /start: <b>{stats.get('starts', 0)}</b>\n\n"
        f"🛒 Открытий раздела покупателя: <b>{stats.get('buyer_opens', 0)}</b>\n"
        f"🏪 Открытий раздела продавца: <b>{stats.get('seller_opens', 0)}</b>\n"
        f"💎 Открытий раздела ТТК: <b>{stats.get('ttk_opens', 0)}</b>\n"
        f"📄 Открытий коммерческого предложения: <b>{stats.get('commercial_opens', 0)}</b>\n\n"
        f"🚀 Переходов на платформу: <b>{stats.get('platform_clicks', 0)}</b>\n\n"
        "📣 Для рассылки используй /broadcast\n"
        "📊 Статистика обновляется в реальном времени"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Отслеживаем открытие WebApp
    if query.data == "commercial":
        await increment_stat("webapp_opens")
    
    await render_screen(query.data, update, context)


# ─────────────────────────────────────────────
# HANDLERS — BROADCAST CONVERSATION (с rate limiting)
# ─────────────────────────────────────────────

async def cmd_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    user_ids = await get_all_user_ids()
    await update.message.reply_text(
        f"📣 <b>Рассылка</b>\n\n"
        f"Аудитория: <b>{len(user_ids)}</b> пользователей.\n\n"
        "Напиши текст сообщения — поддерживается HTML-разметка "
        "(<code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;a href=...&gt;</code>).\n\n"
        "<i>Для отмены отправь /cancel</i>\n"
        "<i>Для отправки фото/видео - отправь медиа с подписью</i>",
        parse_mode="HTML",
    )
    return BROADCAST_WAITING


async def cmd_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return ConversationHandler.END
    
    user_ids = await get_all_user_ids()
    
    status_msg = await update.message.reply_text(
        f"⏳ Отправляю сообщение {len(user_ids)} пользователям...\n"
        f"⏱️ Примерное время: ~{len(user_ids) // 30} секунд"
    )
    
    sent, failed = 0, 0
    message_text = update.message.text or update.message.caption
    
    for i, uid in enumerate(user_ids):
        try:
            if update.message.photo:
                await context.bot.send_photo(
                    chat_id=uid, 
                    photo=update.message.photo[-1].file_id,
                    caption=message_text,
                    parse_mode="HTML"
                )
            elif update.message.video:
                await context.bot.send_video(
                    chat_id=uid,
                    video=update.message.video.file_id,
                    caption=message_text,
                    parse_mode="HTML"
                )
            else:
                await context.bot.send_message(
                    chat_id=uid, 
                    text=message_text, 
                    parse_mode="HTML"
                )
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            logger.warning("Broadcast error for user %d: %s", uid, e)
            failed += 1
        
        # Rate limiting: пауза каждые 30 сообщений
        if (i + 1) % 30 == 0:
            await asyncio.sleep(1)
        
        # Обновляем статус каждые 100 сообщений
        if (i + 1) % 100 == 0:
            await status_msg.edit_text(
                f"⏳ Отправка...\n"
                f"📨 Отправлено: {sent}\n"
                f"❌ Ошибок: {failed}\n"
                f"📊 Прогресс: {i+1}/{len(user_ids)}"
            )
    
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена</b>\n\n"
        f"📨 Доставлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{failed}</b>\n"
        f"📊 Всего: <b>{len(user_ids)}</b> пользователей",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def cmd_broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return ConversationHandler.END
    
    await update.message.reply_text("❌ Рассылка отменена.")
    return ConversationHandler.END


# ─────────────────────────────────────────────
# INLINE MODE HANDLER (инлайн-режим)
# ─────────────────────────────────────────────

async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from telegram import InlineQueryResultArticle, InputTextMessageContent
    
    query = update.inline_query.query
    
    results = [
        InlineQueryResultArticle(
            id="1",
            title="KENTAVR MARKET - Социальный маркетплейс",
            description="Узнай больше о платформе и торговом токене",
            input_message_content=InputTextMessageContent(
                "🚀 <b>KENTAVR MARKET</b>\n\n"
                "Социальный маркетплейс нового поколения с торговым токеном ТТК.\n\n"
                f"Перейти: {PLATFORM_URL}",
                parse_mode="HTML"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Открыть KENTAVR MARKET", url=PLATFORM_URL)],
                [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url=LANDING_URL))],
            ])
        ),
        InlineQueryResultArticle(
            id="2",
            title="Коммерческое предложение KENTAVR",
            description="Подробная информация для партнёров",
            input_message_content=InputTextMessageContent(
                "📄 <b>Коммерческое предложение KENTAVR MARKET</b>\n\n"
                "Откройте полную презентацию проекта в мини-приложении.",
                parse_mode="HTML"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📄 Открыть КП", web_app=WebAppInfo(url=LANDING_URL))],
            ])
        ),
        InlineQueryResultArticle(
            id="3",
            title="Что такое ТТК?",
            description="Торговый токен KENTAVR - объяснение",
            input_message_content=InputTextMessageContent(
                "💎 <b>Торговый Токен KENTAVR (ТТК)</b>\n\n"
                "Внутренний цифровой инструмент экосистемы для бонусов и cashback.\n\n"
                "Подробнее: https://kentavr.world/ttk",
                parse_mode="HTML"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💎 Узнать про ТТК", callback_data="ttk")],
            ])
        )
    ]
    
    await update.inline_query.answer(results, cache_time=300)


# ─────────────────────────────────────────────
# WEB APP HANDLER
# ─────────────────────────────────────────────

async def web_app_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.message.web_app_data
    user_id = update.effective_user.id
    
    await increment_stat("webapp_opens")
    
    if data and data.data:
        logger.info(f"WebApp data from user {user_id}: {data.data[:100]}")
        await update.message.reply_text(
            "✅ Спасибо! Мы получили вашу информацию.\n"
            "Наш менеджер свяжется с вами в ближайшее время."
        )
    else:
        logger.info(f"WebApp opened by user {user_id}")
        await update.message.reply_text(
            "📄 Вы открыли коммерческое предложение KENTAVR MARKET.\n\n"
            "Ознакомьтесь с информацией и возвращайтесь!\n"
            "Если останутся вопросы, напишите нам."
        )


# ─────────────────────────────────────────────
# ERROR HANDLER
# ─────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

async def async_main():
    """Асинхронная инициализация и запуск бота"""
    await init_db()
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)

    broadcast_handler = ConversationHandler(
        entry_points=[CommandHandler("broadcast", cmd_broadcast_start)],
        states={
            BROADCAST_WAITING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_broadcast_send),
                MessageHandler(filters.PHOTO, cmd_broadcast_send),
                MessageHandler(filters.VIDEO, cmd_broadcast_send),
            ]
        },
        fallbacks=[CommandHandler("cancel", cmd_broadcast_cancel)],
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(broadcast_handler)
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_handler))
    app.add_handler(InlineQueryHandler(inline_query_handler))

    logger.info("Bot started successfully. Polling...")
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Add it to environment secrets.")
    
    if not ADMIN_IDS:
        logger.warning("⚠️ ADMIN_IDS not set! /admin command will be available to everyone!")
    
    # Удаляем старые вебхуки перед запуском
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", 
                     json={"drop_pending_updates": True}, timeout=5)
        logger.info("Webhook cleared")
    except Exception as e:
        logger.warning(f"Webhook clear failed: {e}")

    # Запускаем health server в отдельном потоке
    health_thread = threading.Thread(target=_start_health_server, daemon=True)
    health_thread.start()

    # Запускаем основную асинхронную функцию
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")


if __name__ == "__main__":
    main()