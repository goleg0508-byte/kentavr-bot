import os
import logging
import asyncio
import threading
import aiosqlite
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

BOT_TOKEN = os.getenv("BOT_TOKEN", "8326367075:AAFjq1Wknv_lvhrinuNkDIaEwiTShJ3NEA8").strip()
PLATFORM_URL = os.getenv("PLATFORM_URL", "https://kentavr.world/?ref=kentavrmarket").strip()
DB_PATH = "kentavr_stats.db"

BROADCAST_WAITING = 1

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


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
        [InlineKeyboardButton("🚀 Перейти на KENTAVR MARKET", callback_data="goto_platform")],
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
        return
    query = update.callback_query
    try:
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML")
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await asyncio.gather(increment_stat("starts"), register_user(user_id))
    await render_screen("main", update, context, is_new=True)
    return ConversationHandler.END


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await get_stats()
    text = (
        "📊 <b>Статистика KENTAVR MARKET Bot</b>\n\n"
        f"👥 Уникальных пользователей: <b>{stats.get('unique_users', 0)}</b>\n"
        f"▶️ Запусков /start: <b>{stats.get('starts', 0)}</b>\n\n"
        f"🛒 Открытий раздела покупателя: <b>{stats.get('buyer_opens', 0)}</b>\n"
        f"🏪 Открытий раздела продавца: <b>{stats.get('seller_opens', 0)}</b>\n"
        f"💎 Открытий раздела ТТК: <b>{stats.get('ttk_opens', 0)}</b>\n\n"
        f"🚀 Переходов на платформу: <b>{stats.get('platform_clicks', 0)}</b>\n\n"
        "📣 Для рассылки используй /broadcast"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await render_screen(query.data, update, context)


async def cmd_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_ids = await get_all_user_ids()
    await update.message.reply_text(
        f"📣 <b>Рассылка</b>\n\nАудитория: <b>{len(user_ids)}</b> пользователей.\n\n"
        "Напиши текст сообщения (поддерживается HTML).\n\n<i>Для отмены: /cancel</i>",
        parse_mode="HTML",
    )
    return BROADCAST_WAITING


async def cmd_broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_text = update.message.text
    user_ids = await get_all_user_ids()
    status_msg = await update.message.reply_text(f"⏳ Отправляю {len(user_ids)} пользователям...")
    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=message_text, parse_mode="HTML")
            sent += 1
        except Forbidden:
            failed += 1
        except TelegramError as e:
            logger.warning("Broadcast error for %d: %s", uid, e)
            failed += 1
    await status_msg.edit_text(
        f"✅ <b>Рассылка завершена</b>\n\n📨 Доставлено: <b>{sent}</b>\n❌ Ошибок: <b>{failed}</b>",
        parse_mode="HTML",
    )
    return ConversationHandler.END


async def cmd_broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Отменено.")
    return ConversationHandler.END


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception: %s", context.error, exc_info=context.error)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set.")
    threading.Thread(target=_start_health_server, daemon=True).start()
    asyncio.get_event_loop().run_until_complete(init_db())
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_error_handler(error_handler)
    broadcast_handler = ConversationHandler(
        entry_points=[CommandHandler("broadcast", cmd_broadcast_start)],
        states={BROADCAST_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_broadcast_send)]},
        fallbacks=[CommandHandler("cancel", cmd_broadcast_cancel)],
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(broadcast_handler)
    app.add_handler(CallbackQueryHandler(button_handler))
    logger.info("Bot started successfully. Polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
