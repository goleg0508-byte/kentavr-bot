import os
import logging
import asyncio
import threading
import aiosqlite
import json
import urllib.request
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

BOT_TOKEN = os.getenv("BOT_TOKEN", "8857711392:AAFswpbKl3dBwA0LX5aBvvWZ0Q9WmtM3Bqo").strip()
PLATFORM_URL = os.getenv("PLATFORM_URL", "https://kentavr.world/?ref=kentavrmarket").strip()
DB_PATH = "kentavr_stats.db"

BROADCAST_WAITING = 1

# Cache for Telegraph article URL (created once on first use)
_telegraph_url: str | None = None


def _get_telegraph_token() -> str:
    """Create a new Telegraph account and return its access token."""
    payload = json.dumps({
        "short_name": "KentavrMarket",
        "author_name": "KENTAVR MARKET",
        "author_url": PLATFORM_URL,
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.telegra.ph/createAccount",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read())
    if result.get("ok"):
        return result["result"]["access_token"]
    raise RuntimeError(f"Telegraph createAccount error: {result}")


def _create_telegraph_page() -> str:
    """Create a Telegraph page with the commercial proposal and return its URL."""
    content = [
        {"tag": "p", "children": [
            {"tag": "b", "children": ["KENTAVR — это альтернативный виртуальный рынок,"]},
            " созданный на основе принципиально новой бизнес-модели и делового мышления."
        ]},
        {"tag": "p", "children": [
            "KENTAVR — это StartUp, социальный экспериментальный проект, в котором могут принять участие "
            "все желающие, независимо от юридического и социального статуса, профессии и гражданства."
        ]},
        {"tag": "p", "children": [
            "Каждый зарегистрированный пользователь становится ",
            {"tag": "b", "children": ["совладельцем платформы"]},
            ", имеет личный кабинет, счета для внутренней виртуальной валюты и право на получение "
            "своей доли дохода от капитализации маркетплейса."
        ]},
        {"tag": "p", "children": [
            {"tag": "b", "children": ["ТТК (Торговый Токен KENTAVR)"]},
            " — программно-цифровой продукт, созданный с использованием МАК "
            "(Математический Алгоритм Капитализации) для контроля товарооборота и "
            "автоматического распределения дохода между участниками."
        ]},
        {"tag": "h3", "children": ["📋 Условия для продавцов"]},
        {"tag": "p", "children": [
            {"tag": "b", "children": ["1. Бесплатная регистрация"]},
            " — оформление карточек товаров, продуктов, услуг или интеллектуальной собственности без каких-либо взносов."
        ]},
        {"tag": "p", "children": [
            {"tag": "b", "children": ["2. Единый административный сбор — 10%"]},
            " от суммы продаж."
        ]},
        {"tag": "blockquote", "children": [
            "Пример: продано товаров на 100 000 ₽ → сбор составит 10 000 ₽."
        ]},
        {"tag": "p", "children": [
            {"tag": "b", "children": ["3. Кэшбэк для покупателей — от 10% до 50%"]},
            " от стоимости товара в виде ТТК."
        ]},
        {"tag": "blockquote", "children": [
            "Пример: товар стоит 1 000 ₽, кэшбэк 20% → покупатель получает ТТК на сумму 200 ₽ "
            "по актуальному курсу. Если цена ТТК = 100 ₽, покупатель получит 2 ТТК на свой счёт."
        ]},
        {"tag": "p", "children": [
            {"tag": "b", "children": ["4. Право продавца на покупку ТТК"]},
            " — в размере установленного кэшбэка."
        ]},
        {"tag": "blockquote", "children": [
            "Пример: если кэшбэк для покупателя составляет 200 ₽, продавец может приобрести "
            "ТТК на эту же сумму по актуальной цене на дату покупки."
        ]},
        {"tag": "h3", "children": ["🚚 Доставка"]},
        {"tag": "p", "children": [
            "На первом этапе за доставку товаров отвечает продавец. "
            "По мере роста платформы администрация будет привлекать логистических партнёров, "
            "оптимизировать цены на доставку и организовывать пункты выдачи заказов (ПВЗ)."
        ]},
        {"tag": "h3", "children": ["🚀 Присоединяйтесь к KENTAVR MARKET"]},
        {"tag": "p", "children": [
            "Станьте частью социального маркетплейса нового поколения — "
            "регистрация бесплатна, участие открыто для всех."
        ]},
        {"tag": "p", "children": [
            {"tag": "a", "attrs": {"href": PLATFORM_URL}, "children": ["👉 Перейти на платформу KENTAVR MARKET"]}
        ]},
    ]

    token = _get_telegraph_token()

    payload = json.dumps({
        "access_token": token,
        "title": "Коммерческое предложение — KENTAVR MARKET",
        "author_name": "KENTAVR MARKET",
        "author_url": PLATFORM_URL,
        "content": json.dumps(content),
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.telegra.ph/createPage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    if data.get("ok"):
        return data["result"]["url"]
    raise RuntimeError(f"Telegraph API error: {data}")


async def get_telegraph_url() -> str:
    """Return cached Telegraph URL, creating the page if needed."""
    global _telegraph_url
    if _telegraph_url is None:
        loop = asyncio.get_event_loop()
        _telegraph_url = await loop.run_in_executor(None, _create_telegraph_page)
        logger.info("Telegraph page created: %s", _telegraph_url)
    return _telegraph_url

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HEALTH SERVER (required for Replit VM deployment)
# ─────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def _start_health_server():
    port = int(os.getenv("PORT", "8080"))
    try:
        HTTPServer(("", port), _HealthHandler).serve_forever()
    except OSError as e:
        logger.warning("Health server could not start on port %s: %s", port, e)


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS unique_users (
                user_id INTEGER PRIMARY KEY
            )
        """)
        keys = (
            "starts", "buyer_opens", "seller_opens", "ttk_opens",
            "platform_clicks",
        )
        for key in keys:
            await db.execute(
                "INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,)
            )
        await db.commit()


async def increment_stat(key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO stats (key, value) VALUES (?, 1) "
            "ON CONFLICT(key) DO UPDATE SET value = value + 1",
            (key,),
        )
        await db.commit()


async def register_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO unique_users (user_id) VALUES (?)", (user_id,)
        )
        await db.commit()


async def get_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT key, value FROM stats")
        rows = await cursor.fetchall()
        stats = {row[0]: row[1] for row in rows}
        cursor2 = await db.execute("SELECT COUNT(*) FROM unique_users")
        row2 = await cursor2.fetchone()
        stats["unique_users"] = row2[0] if row2 else 0
    return stats


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM unique_users")
        rows = await cursor.fetchall()
    return [row[0] for row in rows]


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
        [InlineKeyboardButton("📄 Коммерческое предложение", callback_data="commercial")],
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
        "ТТК — это не криптовалюта и не инвестиционный инструмент.\n\n"
        "<blockquote>Это внутренний цифровой инструмент платформы, созданный для учёта "
        "активности участников и распределения части дохода от товарооборота.</blockquote>\n\n"
        "Он применяется внутри системы как мера участия: чем активнее ты вовлечён "
        "в деловую среду сообщества, тем больше ТТК накапливается на твоём счёте.\n\n"
        "<i>Что тебя интересует больше?</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🔍 Как устроен ТТК", callback_data="ttk_crypto")],
        [InlineKeyboardButton("💡 Зачем он нужен", callback_data="ttk_benefit")],
        [InlineKeyboardButton("⭐ Чем отличается", callback_data="ttk_unique")],
        [InlineKeyboardButton("🚀 Как начать", callback_data="ttk_start")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_crypto():
    text = (
        "<b>🔍 Как устроен ТТК</b>\n\n"
        "ТТК создан на основе <b>МАК — Математического Алгоритма Капитализации</b>.\n\n"
        "<blockquote>Это программный механизм, который автоматически фиксирует каждую сделку "
        "на платформе и начисляет ТТК участникам пропорционально их вкладу в общий "
        "товарооборот.</blockquote>\n\n"
        "Никакого ручного управления распределением дохода — всё работает по заданным "
        "правилам алгоритма.\n\n"
        "<i>Это делает систему прозрачной и предсказуемой для всех участников.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="ttk")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_benefit():
    text = (
        "<b>💡 Зачем нужен ТТК</b>\n\n"
        "ТТК выполняет несколько функций внутри платформы.\n\n"
        "<blockquote>Во-первых, он фиксирует участие в товарообороте — как покупателя, "
        "так и продавца. Во-вторых, он используется как инструмент cashback: "
        "покупатель получает ТТК с каждой покупки.</blockquote>\n\n"
        "Накопленные ТТК отражают твой вклад в развитие сообщества и могут применяться "
        "в рамках внутренней экономики платформы.\n\n"
        "<i>Чем активнее участие — тем больше возможностей открывается.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="ttk")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


def screen_ttk_unique():
    text = (
        "<b>⭐ Чем отличается ТТК</b>\n\n"
        "Большинство бонусных программ работают в одностороннем порядке: "
        "компания начисляет баллы, пользователь их тратит.\n\n"
        "<blockquote>В KENTAVR MARKET ТТК отражает участие в общей системе товарооборота. "
        "Это не просто скидочный инструмент, а элемент модели, где активность каждого "
        "участника влияет на развитие платформы в целом.</blockquote>\n\n"
        "Покупатели, продавцы и партнёры объединены одной системой сотрудничества и "
        "внутреннего обмена ценностью.\n\n"
        "<i>Это выходит за рамки привычного формата торговой площадки.</i>"
    )
    keyboard = [
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
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
# RENDER ENGINE
# ─────────────────────────────────────────────

async def render_screen(
    screen_key: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    is_new: bool = False,
):
    builder = SCREENS.get(screen_key, screen_main)
    text, markup = builder()

    if screen_key in STAT_MAP:
        await increment_stat(STAT_MAP[screen_key])

    if is_new:
        await update.message.reply_text(text, reply_markup=markup, parse_mode="HTML")
       