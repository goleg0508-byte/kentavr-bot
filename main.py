import os
import io
import csv
import logging
import asyncio
import threading
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

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

# ─── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("kentavr")

# ─── Конфигурация ───────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
DB_PATH      = "bot.db"
HTTP_PORT    = int(os.getenv("PORT", "8080"))

SCREEN_IMAGES = {
    "main":    "https://i.postimg.cc/RFsmw06x/Chat-GPT-Image-4-iun-2026-g-06-12-26.png",
    "buyer":   "https://i.postimg.cc/wTSw7dBP/Chat-GPT-Image-4-iun-2026-g-06-35-43.png",
    "seller":  "https://i.postimg.cc/TwFDCfFH/IMG-20260604-035610-329.png",
    "partner": "https://i.postimg.cc/fT5gqd27/Chat-GPT-Image-4-iun-2026-g-03-57-21.png",
}

SCREEN_TEXTS = {
    "main": (
        "👋 <b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b> — социальный маркетплейс.\n\n"
        "<i>Кем ты являешься?</i>"
    ),
    "buyer": (
        "🛒 <b>Для покупателей</b>\n\n"
        "Покупай у проверенных участников и получай "
        "<b>кэшбэк в ТТК</b> за каждую покупку.\n\n"
        "Чем активнее — тем больше возможностей."
    ),
    "seller": (
        "🏪 <b>Для продавцов</b>\n\n"
        "Размести товары или услуги и получи доступ к активной аудитории.\n\n"
        "Долгосрочные отношения, не разовые сделки."
    ),
    "partner": (
        "💎 <b>Партнёр / ТТК</b>\n\n"
        "Участвуй в развитии платформы и получай "
        "<b>Торговый Токен KENTAVR (ТТК)</b>.\n\n"
        "Реальная внутренняя экономика — ценность растёт "
        "вместе с товарооборотом."
    ),
}

SCREEN_NAMES = {
    "main":    "🏠 Главный экран",
    "buyer":   "🛒 Покупатель",
    "seller":  "🏪 Продавец",
    "partner": "💎 Партнёр / ТТК",
}

# ─── Кэш картинок ───────────────────────────────────────────────────────────────
# Словарь: url (str) → байты (bytes)
IMAGE_CACHE: dict = {}

# ─── База данных ─────────────────────────────────────────────────────────────────
PG_POOL  = None
PG_MODE  = False


async def db_init():
    global PG_POOL, PG_MODE

    if DATABASE_URL:
        try:
            import asyncpg
            PG_POOL = await asyncio.wait_for(
                asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5),
                timeout=10,
            )
            async with PG_POOL.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        key TEXT PRIMARY KEY,
                        val BIGINT DEFAULT 0
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        uid       BIGINT PRIMARY KEY,
                        joined_at TIMESTAMPTZ DEFAULT NOW(),
                        seen_at   TIMESTAMPTZ DEFAULT NOW(),
                        actions   INT DEFAULT 0
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS custom_content (
                        screen_key TEXT PRIMARY KEY,
                        custom_txt TEXT,
                        custom_img TEXT
                    )
                """)
                for key in ("starts", "buyer", "seller", "partner"):
                    await conn.execute(
                        "INSERT INTO stats(key, val) VALUES($1, 0) ON CONFLICT DO NOTHING",
                        key,
                    )
            PG_MODE = True
            log.info("БД: PostgreSQL ✓")
            return
        except Exception as err:
            log.warning(f"БД: PostgreSQL недоступен ({err}), переключаюсь на SQLite")
            PG_POOL = None

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                val INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                uid       INTEGER PRIMARY KEY,
                joined_at TEXT,
                seen_at   TEXT,
                actions   INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS custom_content (
                screen_key TEXT PRIMARY KEY,
                custom_txt TEXT,
                custom_img TEXT
            )
        """)
        for key in ("starts", "buyer", "seller", "partner"):
            await db.execute(
                "INSERT OR IGNORE INTO stats(key, val) VALUES(?, 0)", (key,)
            )
        await db.commit()
    log.info("БД: SQLite ✓")


async def db_increment(key: str):
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                await conn.execute(
                    "INSERT INTO stats(key, val) VALUES($1, 1) "
                    "ON CONFLICT(key) DO UPDATE SET val = stats.val + 1",
                    key,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO stats(key, val) VALUES(?, 0)", (key,))
                await db.execute("UPDATE stats SET val = val + 1 WHERE key = ?", (key,))
                await db.commit()
    except Exception as err:
        log.warning(f"db_increment({key}): {err}")


async def db_register_user(uid: int):
    try:
        now = datetime.utcnow().isoformat()
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                await conn.execute(
                    "INSERT INTO users(uid) VALUES($1) "
                    "ON CONFLICT(uid) DO UPDATE SET seen_at = NOW(), actions = users.actions + 1",
                    uid,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                row = await (
                    await db.execute("SELECT uid FROM users WHERE uid = ?", (uid,))
                ).fetchone()
                if row:
                    await db.execute(
                        "UPDATE users SET seen_at = ?, actions = actions + 1 WHERE uid = ?",
                        (now, uid),
                    )
                else:
                    await db.execute(
                        "INSERT INTO users(uid, joined_at, seen_at, actions) VALUES(?, ?, ?, 1)",
                        (uid, now, now),
                    )
                await db.commit()
    except Exception as err:
        log.warning(f"db_register_user: {err}")


async def db_get_stats() -> dict:
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                rows = await conn.fetch("SELECT key, val FROM stats")
                result = {r["key"]: r["val"] for r in rows}
                result["total_users"]  = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
                result["active_today"] = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE seen_at::date = CURRENT_DATE"
                ) or 0
                result["total_actions"] = await conn.fetchval("SELECT SUM(actions) FROM users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                rows = await (await db.execute("SELECT key, val FROM stats")).fetchall()
                result = {r[0]: r[1] for r in rows}
                result["total_users"] = (
                    await (await db.execute("SELECT COUNT(*) FROM users")).fetchone()
                )[0] or 0
                result["active_today"] = (
                    await (
                        await db.execute(
                            "SELECT COUNT(*) FROM users WHERE date(seen_at) = date('now')"
                        )
                    ).fetchone()
                )[0] or 0
                r = await (await db.execute("SELECT SUM(actions) FROM users")).fetchone()
                result["total_actions"] = r[0] if r and r[0] else 0
        return result
    except Exception as err:
        log.warning(f"db_get_stats: {err}")
        return {}


async def db_get_all_uids() -> list:
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                rows = await conn.fetch("SELECT uid FROM users")
                return [r["uid"] for r in rows]
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            rows = await (await db.execute("SELECT uid FROM users")).fetchall()
            return [r[0] for r in rows]
    except Exception:
        return []


async def db_get_custom(screen_key: str):
    """Возвращает (custom_txt, custom_img) или (None, None)."""
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT custom_txt, custom_img FROM custom_content WHERE screen_key = $1",
                    screen_key,
                )
                return (row["custom_txt"], row["custom_img"]) if row else (None, None)
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            row = await (
                await db.execute(
                    "SELECT custom_txt, custom_img FROM custom_content WHERE screen_key = ?",
                    (screen_key,),
                )
            ).fetchone()
            return (row[0], row[1]) if row else (None, None)
    except Exception:
        return (None, None)


async def db_set_custom_text(screen_key: str, text: str):
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                await conn.execute(
                    "INSERT INTO custom_content(screen_key, custom_txt) VALUES($1, $2) "
                    "ON CONFLICT(screen_key) DO UPDATE SET custom_txt = $2",
                    screen_key, text,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO custom_content(screen_key) VALUES(?)", (screen_key,)
                )
                await db.execute(
                    "UPDATE custom_content SET custom_txt = ? WHERE screen_key = ?",
                    (text, screen_key),
                )
                await db.commit()
    except Exception as err:
        log.warning(f"db_set_custom_text: {err}")


async def db_reset_custom_text(screen_key: str):
    try:
        if PG_MODE and PG_POOL:
            async with PG_POOL.acquire() as conn:
                await conn.execute(
                    "UPDATE custom_content SET custom_txt = NULL WHERE screen_key = $1",
                    screen_key,
                )
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE custom_content SET custom_txt = NULL WHERE screen_key = ?",
                    (screen_key,),
                )
                await db.commit()
    except Exception as err:
        log.warning(f"db_reset_custom_text: {err}")


# ─── Картинки ────────────────────────────────────────────────────────────────────

def _download_image_sync(url: str) -> bytes:
    """Синхронная загрузка картинки (запускается в отдельном потоке)."""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read()


async def get_image_bytes(url: str):
    """Возвращает байты картинки из кэша или скачивает."""
    if url in IMAGE_CACHE:
        return IMAGE_CACHE[url]
    try:
        data = await asyncio.to_thread(_download_image_sync, url)
        IMAGE_CACHE[url] = data
        log.info(f"Картинка загружена: {len(data) // 1024} KB")
        return data
    except Exception as err:
        log.warning(f"Не удалось загрузить картинку: {err}")
        return None


async def preload_images_background():
    """Загружает все картинки в фоне — не блокирует старт бота."""
    await asyncio.sleep(2)  # подождать пока polling стартует
    log.info("Фоновая загрузка картинок...")
    for name, url in SCREEN_IMAGES.items():
        await get_image_bytes(url)
        log.info(f"  ✓ {name}")
    log.info(f"Готово: {len(IMAGE_CACHE)}/{len(SCREEN_IMAGES)} картинок в кэше")


# ─── Клавиатуры ──────────────────────────────────────────────────────────────────

def make_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Я покупатель",         callback_data="go:buyer")],
        [InlineKeyboardButton("🏪 Я продавец",           callback_data="go:seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром", callback_data="go:partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton(
            "📄 Коммерческое предложение",
            web_app=WebAppInfo(url="https://kentavrmarket.shop"),
        )],
    ])


def make_screen_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])


def make_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Статистика",   callback_data="adm:stats"),
            InlineKeyboardButton("👥 Пользователи", callback_data="adm:users"),
        ],
        [
            InlineKeyboardButton("📢 Рассылка", callback_data="adm:broadcast"),
            InlineKeyboardButton("📥 Экспорт",  callback_data="adm:export"),
        ],
        [InlineKeyboardButton("✏️ Редактировать тексты", callback_data="adm:texts")],
        [InlineKeyboardButton("🏠 На главную",            callback_data="go:main")],
    ])


def make_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]
    ])


# ─── Отправка экранов ─────────────────────────────────────────────────────────────

async def send_screen(bot, chat_id: int, screen_key: str):
    """Отправляет экран с картинкой (или текстом если картинка недоступна)."""
    custom_txt, custom_img = await db_get_custom(screen_key)

    text   = custom_txt or SCREEN_TEXTS.get(screen_key, SCREEN_TEXTS["main"])
    markup = make_main_keyboard() if screen_key == "main" else make_screen_keyboard()
    img_url = custom_img or SCREEN_IMAGES.get(screen_key)

    if img_url:
        img_bytes = await get_image_bytes(img_url)
        if img_bytes:
            try:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=io.BytesIO(img_bytes),
                    caption=text,
                    reply_markup=markup,
                    parse_mode="HTML",
                )
                return
            except Exception as err:
                log.warning(f"send_photo не удалось ({screen_key}): {err}")

    # Fallback: текст без картинки
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=markup,
        parse_mode="HTML",
    )


async def send_admin_panel(bot, chat_id: int, message_id: int = None):
    stats = await db_get_stats()
    text = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{stats.get('total_users', 0)}</b>   "
        f"📅 Сегодня: <b>{stats.get('active_today', 0)}</b>\n"
        f"🎯 Действий всего: <b>{stats.get('total_actions', 0)}</b>\n\n"
        "<i>Выбери действие:</i>"
    )
    keyboard = make_admin_keyboard()

    if message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            return
        except Exception:
            pass

    await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="HTML")


# ─── Хэндлеры команд ─────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log.info(f"/start от uid={uid}")
    ctx.user_data.clear()
    await asyncio.gather(
        db_increment("starts"),
        db_register_user(uid),
    )
    await send_screen(ctx.bot, update.effective_chat.id, "main")


async def cmd_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    ctx.user_data.clear()
    await send_admin_panel(ctx.bot, update.effective_chat.id)


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ Отменено.",
        reply_markup=make_back_keyboard(),
    )


# ─── Хэндлер кнопок ──────────────────────────────────────────────────────────────

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    uid      = update.effective_user.id
    chat_id  = update.effective_chat.id
    msg_id   = query.message.message_id
    data     = query.data
    bot      = ctx.bot

    log.info(f"Кнопка: uid={uid} data={data!r}")

    # Сразу убираем "часики" с кнопки
    await query.answer()

    # ── Навигация по экранам (доступна всем) ─────────────────────────────────
    if data.startswith("go:"):
        screen_key = data[3:]
        ctx.user_data.clear()

        if screen_key in ("buyer", "seller", "partner"):
            await db_increment(screen_key)

        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

        await send_screen(bot, chat_id, screen_key)
        return

    # ── Только для администраторов ────────────────────────────────────────────
    if uid not in ADMIN_IDS:
        return

    # Вспомогательная функция: редактировать или отправить новое сообщение
    async def reply(text: str, keyboard: InlineKeyboardMarkup):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )
        except Exception:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="HTML",
            )

    back_kb = make_back_keyboard()

    # ── Панель ───────────────────────────────────────────────────────────────
    if data == "adm:panel":
        ctx.user_data.clear()
        await send_admin_panel(bot, chat_id, msg_id)

    # ── Статистика ────────────────────────────────────────────────────────────
    elif data == "adm:stats":
        stats = await db_get_stats()
        text = (
            "📊 <b>СТАТИСТИКА</b>\n━━━━━━━━━━━━━━━━\n\n"
            f"👥 Пользователей: <code>{stats.get('total_users', 0)}</code>\n"
            f"📅 Активных сегодня: <code>{stats.get('active_today', 0)}</code>\n"
            f"🎯 Действий всего: <code>{stats.get('total_actions', 0)}</code>\n\n"
            f"▶️ /start нажато: <code>{stats.get('starts', 0)}</code>\n"
            f"🛒 Покупатель: <code>{stats.get('buyer', 0)}</code>\n"
            f"🏪 Продавец: <code>{stats.get('seller', 0)}</code>\n"
            f"💎 Партнёр: <code>{stats.get('partner', 0)}</code>"
        )
        await reply(text, back_kb)

    # ── Пользователи ──────────────────────────────────────────────────────────
    elif data == "adm:users":
        uids = await db_get_all_uids()
        text = f"👥 <b>Пользователи</b> — всего: <code>{len(uids)}</code>\n\n"
        for i, u in enumerate(uids[-20:][::-1], 1):
            text += f"{i}. <code>{u}</code>\n"
        await reply(text, back_kb)

    # ── Экспорт CSV ───────────────────────────────────────────────────────────
    elif data == "adm:export":
        uids  = await db_get_all_uids()
        stats = await db_get_stats()
        buf   = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["Метрика", "Значение"])
        for key, label in [
            ("total_users",   "Пользователей"),
            ("active_today",  "Активных сегодня"),
            ("total_actions", "Действий всего"),
            ("starts",        "/start"),
            ("buyer",         "Покупатель"),
            ("seller",        "Продавец"),
            ("partner",       "Партнёр"),
        ]:
            writer.writerow([label, stats.get(key, 0)])
        writer.writerow([])
        writer.writerow(["user_id"])
        for u in uids:
            writer.writerow([u])
        buf.seek(0)
        await bot.send_document(
            chat_id=chat_id,
            document=io.BytesIO(buf.getvalue().encode("utf-8")),
            filename=f"kentavr_{datetime.now():%Y%m%d_%H%M}.csv",
            caption="📊 Экспорт KENTAVR MARKET",
        )

    # ── Рассылка ──────────────────────────────────────────────────────────────
    elif data == "adm:broadcast":
        uids = await db_get_all_uids()
        ctx.user_data["state"] = "broadcast"
        cancel_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]
        ])
        await reply(
            f"📢 <b>Рассылка</b>\n\n"
            f"Получателей: <b>{len(uids)} чел.</b>\n\n"
            "Напиши текст сообщения и отправь.\n"
            "/cancel — отмена.",
            cancel_kb,
        )

    # ── Тексты экранов ────────────────────────────────────────────────────────
    elif data == "adm:texts":
        ctx.user_data["state"] = "texts_menu"
        rows = [
            [InlineKeyboardButton(name, callback_data=f"adm:edit:{key}")]
            for key, name in SCREEN_NAMES.items()
        ]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        await reply("✏️ <b>Тексты экранов</b>\n\nВыбери экран:", InlineKeyboardMarkup(rows))

    elif data.startswith("adm:edit:"):
        screen_key = data[9:]
        ctx.user_data["state"]      = "edit_text"
        ctx.user_data["screen_key"] = screen_key

        custom_txt, _ = await db_get_custom(screen_key)
        current = custom_txt or SCREEN_TEXTS.get(screen_key, "")
        preview = current[:400] + ("…" if len(current) > 400 else "")
        label   = "изменён" if custom_txt else "оригинал"

        rows = []
        if custom_txt:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:reset:{screen_key}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:texts")])

        await reply(
            f"✏️ <b>{SCREEN_NAMES.get(screen_key, screen_key)}</b> ({label})\n\n"
            f"<b>Текущий текст:</b>\n<blockquote>{preview}</blockquote>\n\n"
            "Отправь новый текст. /cancel — отмена.",
            InlineKeyboardMarkup(rows),
        )

    elif data.startswith("adm:reset:"):
        screen_key = data[10:]
        await db_reset_custom_text(screen_key)
        ctx.user_data.clear()
        await reply(
            f"🔄 Текст «{SCREEN_NAMES.get(screen_key, screen_key)}» сброшен на оригинал.",
            back_kb,
        )


# ─── Хэндлер текстовых сообщений (для админ-флоуов) ─────────────────────────────

async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = ctx.user_data.get("state")

    if uid not in ADMIN_IDS or not state:
        return

    back_kb = make_back_keyboard()

    if state == "broadcast":
        uids     = await db_get_all_uids()
        progress = await update.message.reply_text(
            f"⏳ Рассылаю {len(uids)} пользователям…"
        )
        sent = failed = 0
        for i, user_id in enumerate(uids):
            try:
                await ctx.bot.send_message(user_id, text, parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            if (i + 1) % 30 == 0:
                await asyncio.sleep(1)
        ctx.user_data.clear()
        await progress.edit_text(
            f"✅ Доставлено: <b>{sent}</b>\n❌ Не доставлено: <b>{failed}</b>",
            reply_markup=back_kb,
            parse_mode="HTML",
        )

    elif state == "edit_text":
        screen_key = ctx.user_data.get("screen_key")
        if not screen_key:
            return
        await db_set_custom_text(screen_key, text)
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Текст «{SCREEN_NAMES.get(screen_key, screen_key)}» обновлён!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:texts")],
                [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
            ]),
            parse_mode="HTML",
        )


async def handle_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Ошибка бота: {ctx.error}", exc_info=ctx.error)


# ─── Инициализация ────────────────────────────────────────────────────────────────

async def on_startup(app: Application):
    """Вызывается перед стартом polling. Только БД — быстро."""
    try:
        await db_init()
    except Exception as err:
        log.error(f"db_init: {err}")

    # Картинки грузим В ФОНЕ — не блокируем старт
    asyncio.create_task(preload_images_background())


# ─── Health-check для Railway ─────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass  # не засорять логи


# ─── Точка входа ──────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("  KENTAVR MARKET Bot")
    log.info(f"  BOT_TOKEN:    {'✓ задан' if BOT_TOKEN else '✗ НЕ ЗАДАН!'}")
    log.info(f"  ADMIN_IDS:    {ADMIN_IDS or 'не заданы'}")
    log.info(f"  DATABASE_URL: {'✓ задан' if DATABASE_URL else 'нет (SQLite)'}")
    log.info("=" * 55)

    if not BOT_TOKEN:
        raise SystemExit("❌ BOT_TOKEN не задан. Добавь в Railway → Variables.")

    # Health-check сервер в отдельном потоке
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), HealthHandler).serve_forever(),
        daemon=True,
    ).start()
    log.info(f"Health-check: порт {HTTP_PORT} ✓")

    # Сборка приложения
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    app.add_error_handler(handle_error)
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("admin",  cmd_admin))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    log.info("Запуск polling ✓")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
