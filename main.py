"""
KENTAVR MARKET — Telegram Bot
Упрощённый: 3 экрана + платформа + КП. Без ConversationHandler.
"""
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
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

load_dotenv()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("kentavr")

# ── Конфигурация ────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS    = [
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
]
DB_PATH = "kentavr.db"

IMAGES = {
    "main":    "https://i.postimg.cc/RFsmw06x/Chat-GPT-Image-4-iun-2026-g-06-12-26.png",
    "buyer":   "https://i.postimg.cc/wTSw7dBP/Chat-GPT-Image-4-iun-2026-g-06-35-43.png",
    "seller":  "https://i.postimg.cc/TwFDCfFH/IMG-20260604-035610-329.png",
    "partner": "https://i.postimg.cc/fT5gqd27/Chat-GPT-Image-4-iun-2026-g-03-57-21.png",
}

TEXTS = {
    "main": (
        "👋 <b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b> — социальный маркетплейс, "
        "где покупатели, продавцы и партнёры работают в единой системе.\n\n"
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
        "Размести товары, услуги или экспертизу и получи "
        "доступ к активной аудитории.\n\n"
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

# Кэш картинок: url → bytes, заполняется при старте
_cache: dict = {}

# ── Глобальное состояние БД ─────────────────────────────────────────────────────
_pool       = None
_use_pg     = False


# ═══════════════════════════════════════════════════════════════════════════════
# Health-check сервер (Railway требует открытый PORT)
# ═══════════════════════════════════════════════════════════════════════════════

class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *_): pass

def _run_health():
    port = int(os.getenv("PORT", "8080"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()


# ═══════════════════════════════════════════════════════════════════════════════
# База данных
# ═══════════════════════════════════════════════════════════════════════════════

async def db_init():
    global _pool, _use_pg
    if DATABASE_URL:
        try:
            import asyncpg
            _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with _pool.acquire() as c:
                await c.execute("""
                    CREATE TABLE IF NOT EXISTS stats (
                        key TEXT PRIMARY KEY, val BIGINT DEFAULT 0)""")
                await c.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        uid BIGINT PRIMARY KEY,
                        first_seen TIMESTAMPTZ DEFAULT NOW(),
                        last_seen  TIMESTAMPTZ DEFAULT NOW(),
                        cnt INT DEFAULT 0)""")
                await c.execute("""
                    CREATE TABLE IF NOT EXISTS content (
                        sk TEXT PRIMARY KEY, txt TEXT, img TEXT)""")
                for k in ("starts", "buyer", "seller", "partner"):
                    await c.execute(
                        "INSERT INTO stats(key,val) VALUES($1,0) ON CONFLICT DO NOTHING", k)
            _use_pg = True
            log.info("DB → PostgreSQL OK")
            return
        except Exception as e:
            log.warning(f"DB → PostgreSQL failed: {e}")
            _pool = None

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, val INTEGER DEFAULT 0)")
        await db.execute(
            "CREATE TABLE IF NOT EXISTS users "
            "(uid INTEGER PRIMARY KEY, first_seen TEXT, last_seen TEXT, cnt INTEGER DEFAULT 0)")
        await db.execute(
            "CREATE TABLE IF NOT EXISTS content "
            "(sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
        for k in ("starts", "buyer", "seller", "partner"):
            await db.execute(
                "INSERT OR IGNORE INTO stats(key,val) VALUES(?,0)", (k,))
        await db.commit()
    log.info("DB → SQLite OK")


async def db_inc(key: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute(
                    "INSERT INTO stats(key,val) VALUES($1,1) "
                    "ON CONFLICT(key) DO UPDATE SET val=stats.val+1", key)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO stats(key,val) VALUES(?,0)", (key,))
                await db.execute("UPDATE stats SET val=val+1 WHERE key=?", (key,))
                await db.commit()
    except Exception as e:
        log.warning(f"db_inc({key}): {e}")


async def db_touch(uid: int):
    try:
        now = datetime.utcnow().isoformat()
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute(
                    "INSERT INTO users(uid) VALUES($1) "
                    "ON CONFLICT(uid) DO UPDATE SET last_seen=NOW(), cnt=users.cnt+1", uid)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                r = (await (await db.execute(
                    "SELECT uid FROM users WHERE uid=?", (uid,))).fetchone())
                if r:
                    await db.execute(
                        "UPDATE users SET last_seen=?,cnt=cnt+1 WHERE uid=?", (now, uid))
                else:
                    await db.execute(
                        "INSERT INTO users(uid,first_seen,last_seen,cnt) VALUES(?,?,?,1)",
                        (uid, now, now))
                await db.commit()
    except Exception as e:
        log.warning(f"db_touch: {e}")


async def db_stats() -> dict:
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                rows = await c.fetch("SELECT key,val FROM stats")
                s = {r["key"]: r["val"] for r in rows}
                s["users"]   = await c.fetchval("SELECT COUNT(*) FROM users") or 0
                s["today"]   = await c.fetchval(
                    "SELECT COUNT(*) FROM users WHERE last_seen::date=CURRENT_DATE") or 0
                s["actions"] = await c.fetchval("SELECT SUM(cnt) FROM users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                rows = await (await db.execute("SELECT key,val FROM stats")).fetchall()
                s = {r[0]: r[1] for r in rows}
                s["users"]  = (await (
                    await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0] or 0
                s["today"]  = (await (
                    await db.execute(
                        "SELECT COUNT(*) FROM users WHERE date(last_seen)=date('now')"
                    )).fetchone())[0] or 0
                r = await (await db.execute("SELECT SUM(cnt) FROM users")).fetchone()
                s["actions"] = r[0] if r and r[0] else 0
        return s
    except Exception as e:
        log.warning(f"db_stats: {e}")
        return {}


async def db_uids() -> list:
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                return [r["uid"] for r in await c.fetch("SELECT uid FROM users")]
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            return [r[0] for r in await (
                await db.execute("SELECT uid FROM users")).fetchall()]
    except Exception:
        return []


async def db_get(sk: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                r = await c.fetchrow("SELECT txt,img FROM content WHERE sk=$1", sk)
                return (r["txt"], r["img"]) if r else (None, None)
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            r = await (await db.execute(
                "SELECT txt,img FROM content WHERE sk=?", (sk,))).fetchone()
            return (r[0], r[1]) if r else (None, None)
    except Exception:
        return (None, None)


async def db_set(sk: str, txt=None, img=None):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                if txt is not None:
                    await c.execute(
                        "INSERT INTO content(sk,txt) VALUES($1,$2) "
                        "ON CONFLICT(sk) DO UPDATE SET txt=$2", sk, txt)
                if img is not None:
                    await c.execute(
                        "INSERT INTO content(sk,img) VALUES($1,$2) "
                        "ON CONFLICT(sk) DO UPDATE SET img=$2", sk, img)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO content(sk) VALUES(?)", (sk,))
                if txt is not None:
                    await db.execute("UPDATE content SET txt=? WHERE sk=?", (txt, sk))
                if img is not None:
                    await db.execute("UPDATE content SET img=? WHERE sk=?", (img, sk))
                await db.commit()
    except Exception as e:
        log.warning(f"db_set: {e}")


async def db_reset(sk: str, col: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute(f"UPDATE content SET {col}=NULL WHERE sk=$1", sk)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(f"UPDATE content SET {col}=NULL WHERE sk=?", (sk,))
                await db.commit()
    except Exception as e:
        log.warning(f"db_reset: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Картинки
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_url(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "TelegramBot/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


async def img_preload():
    log.info("Загрузка картинок...")
    for key, url in IMAGES.items():
        try:
            data = await asyncio.to_thread(_fetch_url, url)
            _cache[url] = data
            log.info(f"  ✓ {key} ({len(data)//1024} KB)")
        except Exception as e:
            log.warning(f"  ✗ {key}: {e}")
    log.info(f"Картинки: {len(_cache)}/{len(IMAGES)} загружено")


async def img_get(url: str):
    if url in _cache:
        return _cache[url]
    try:
        data = await asyncio.to_thread(_fetch_url, url)
        _cache[url] = data
        return data
    except Exception as e:
        log.warning(f"img_get failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Клавиатуры
# ═══════════════════════════════════════════════════════════════════════════════

def kb_main():
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


def kb_screen():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])


def kb_admin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",      callback_data="adm:stats"),
         InlineKeyboardButton("👥 Пользователи",    callback_data="adm:users")],
        [InlineKeyboardButton("📢 Рассылка",        callback_data="adm:broadcast"),
         InlineKeyboardButton("📥 Экспорт",         callback_data="adm:export")],
        [InlineKeyboardButton("✏️ Тексты",          callback_data="adm:texts")],
        [InlineKeyboardButton("🏠 На главную",       callback_data="go:main")],
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# Отправка экранов
# ═══════════════════════════════════════════════════════════════════════════════

async def show_screen(bot, chat_id: int, key: str):
    custom_txt, custom_img = await db_get(key)
    text      = custom_txt or TEXTS.get(key, TEXTS["main"])
    markup    = kb_main() if key == "main" else kb_screen()
    image_url = custom_img or IMAGES.get(key)

    if image_url:
        img_bytes = await img_get(image_url)
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
            except Exception as e:
                log.warning(f"send_photo({key}): {e}")

    await bot.send_message(
        chat_id=chat_id, text=text, reply_markup=markup, parse_mode="HTML")


async def show_admin(bot, chat_id: int, mid: int = None):
    s    = await db_stats()
    text = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{s.get('users', 0)}</b>   "
        f"📅 Сегодня: <b>{s.get('today', 0)}</b>\n"
        f"🎯 Действий всего: <b>{s.get('actions', 0)}</b>\n\n"
        "<i>Выбери действие:</i>"
    )
    if mid:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=mid,
                text=text, reply_markup=kb_admin(), parse_mode="HTML")
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb_admin(), parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════════════════
# Хэндлеры
# ═══════════════════════════════════════════════════════════════════════════════

async def h_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log.info(f"/start uid={uid}")
    ctx.user_data.clear()
    await asyncio.gather(db_inc("starts"), db_touch(uid))
    await show_screen(ctx.bot, update.effective_chat.id, "main")


async def h_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    ctx.user_data.clear()
    await show_admin(ctx.bot, update.effective_chat.id)


async def h_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
    await update.message.reply_text("❌ Отменено.", reply_markup=kb)


async def h_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = update.effective_user.id
    cid  = update.effective_chat.id
    mid  = q.message.message_id
    data = q.data
    bot  = ctx.bot

    log.info(f"[CB] uid={uid} data={data!r}")

    # Сразу отвечаем Telegram чтобы убрать "часики" с кнопки
    await q.answer()

    # ── Навигация (доступна всем) ────────────────────────────────────────────
    if data.startswith("go:"):
        key = data[3:]
        ctx.user_data.clear()
        if key in ("buyer", "seller", "partner"):
            await db_inc(key)
        # Удаляем старое сообщение
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
        except Exception:
            pass
        # Показываем новый экран
        await show_screen(bot, cid, key)
        return

    # ── Только для админов ───────────────────────────────────────────────────
    if uid not in ADMIN_IDS:
        return

    # Вспомогательная функция для edit-or-send
    async def edit(text: str, kb: InlineKeyboardMarkup):
        try:
            await bot.edit_message_text(
                chat_id=cid, message_id=mid,
                text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(
                chat_id=cid, text=text, reply_markup=kb, parse_mode="HTML")

    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])

    if data == "adm:panel":
        ctx.user_data.clear()
        await show_admin(bot, cid, mid)

    elif data == "adm:stats":
        s = await db_stats()
        t = (
            "📊 <b>СТАТИСТИКА</b>\n━━━━━━━━━━━━━━━━\n\n"
            f"👥 Пользователей: <code>{s.get('users',0)}</code>\n"
            f"📅 Активных сегодня: <code>{s.get('today',0)}</code>\n"
            f"🎯 Действий: <code>{s.get('actions',0)}</code>\n\n"
            f"▶️ /start: <code>{s.get('starts',0)}</code>\n"
            f"🛒 Покупатель: <code>{s.get('buyer',0)}</code>\n"
            f"🏪 Продавец: <code>{s.get('seller',0)}</code>\n"
            f"💎 Партнёр: <code>{s.get('partner',0)}</code>"
        )
        await edit(t, back)

    elif data == "adm:users":
        uids = await db_uids()
        t = f"👥 <b>Пользователи</b> — всего: <code>{len(uids)}</code>\n\n"
        for i, u in enumerate(uids[-20:][::-1], 1):
            t += f"{i}. <code>{u}</code>\n"
        await edit(t, back)

    elif data == "adm:export":
        uids = await db_uids()
        s    = await db_stats()
        buf  = io.StringIO()
        w    = csv.writer(buf)
        w.writerow(["Метрика", "Значение"])
        for k, lbl in [
            ("users","Пользователей"), ("today","Сегодня"), ("actions","Действий"),
            ("starts","/start"), ("buyer","Покупатель"), ("seller","Продавец"), ("partner","Партнёр"),
        ]:
            w.writerow([lbl, s.get(k, 0)])
        w.writerow([]); w.writerow(["user_id"])
        for u in uids: w.writerow([u])
        buf.seek(0)
        await bot.send_document(
            chat_id=cid,
            document=io.BytesIO(buf.getvalue().encode()),
            filename=f"kentavr_{datetime.now():%Y%m%d_%H%M}.csv",
            caption="📊 Экспорт")

    elif data == "adm:broadcast":
        uids = await db_uids()
        ctx.user_data["state"] = "broadcast"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]])
        await edit(
            f"📢 <b>Рассылка</b>\n\nАудитория: <b>{len(uids)} чел.</b>\n\n"
            "Отправь текст сообщения. /cancel — отмена.", kb)

    elif data == "adm:texts":
        ctx.user_data["state"] = "texts_menu"
        rows = [[InlineKeyboardButton(name, callback_data=f"adm:t:{k}")]
                for k, name in SCREEN_NAMES.items()]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        await edit("✏️ <b>Тексты экранов</b>\n\nВыбери экран:", InlineKeyboardMarkup(rows))

    elif data.startswith("adm:t:"):
        sk = data[6:]
        ctx.user_data.update({"state": "edit_text", "sk": sk})
        ct, _ = await db_get(sk)
        cur   = ct or TEXTS.get(sk, "")
        prev  = cur[:500] + ("…" if len(cur) > 500 else "")
        lbl   = "изменён" if ct else "оригинал"
        rows  = []
        if ct:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:rt:{sk}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:texts")])
        await edit(
            f"✏️ <b>{SCREEN_NAMES.get(sk, sk)}</b> ({lbl})\n\n"
            f"<b>Текущий текст:</b>\n<blockquote>{prev}</blockquote>\n\n"
            "Отправь новый текст. /cancel — отмена.",
            InlineKeyboardMarkup(rows))

    elif data.startswith("adm:rt:"):
        sk = data[7:]
        await db_reset(sk, "txt")
        ctx.user_data.clear()
        await edit(f"🔄 Текст «{SCREEN_NAMES.get(sk, sk)}» сброшен.", back)


async def h_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = ctx.user_data.get("state")

    if uid not in ADMIN_IDS or not state:
        return

    sk   = ctx.user_data.get("sk")
    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])

    if state == "broadcast":
        uids = await db_uids()
        msg  = await update.message.reply_text(f"⏳ Отправляю {len(uids)} пользователям…")
        sent = failed = 0
        for i, u in enumerate(uids):
            try:
                await ctx.bot.send_message(u, text, parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            if (i + 1) % 30 == 0:
                await asyncio.sleep(1)
        ctx.user_data.clear()
        await msg.edit_text(
            f"✅ Доставлено: <b>{sent}</b>  ❌ Ошибок: <b>{failed}</b>",
            reply_markup=back, parse_mode="HTML")

    elif state == "edit_text" and sk:
        await db_set(sk, txt=text)
        ctx.user_data.clear()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:texts")],
            [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
        ])
        await update.message.reply_text(
            f"✅ Текст <b>{SCREEN_NAMES.get(sk, sk)}</b> обновлён!",
            reply_markup=kb, parse_mode="HTML")


async def h_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Ошибка: {ctx.error}", exc_info=ctx.error)


# ═══════════════════════════════════════════════════════════════════════════════
# Старт
# ═══════════════════════════════════════════════════════════════════════════════

async def on_init(app: Application):
    try:
        await db_init()
    except Exception as e:
        log.error(f"db_init: {e}")
    await img_preload()


def main():
    log.info("=" * 60)
    log.info("KENTAVR MARKET бот стартует")
    log.info(f"BOT_TOKEN:   {'SET ✓' if BOT_TOKEN else 'MISSING ✗'}")
    log.info(f"ADMIN_IDS:   {ADMIN_IDS}")
    log.info(f"DATABASE_URL:{'SET ✓' if DATABASE_URL else 'нет (SQLite)'}")
    log.info("=" * 60)

    if not BOT_TOKEN:
        raise SystemExit("❌ BOT_TOKEN не задан! Добавь в Railway → Variables")

    # Health-check в отдельном потоке
    threading.Thread(target=_run_health, daemon=True).start()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_init)
        .build()
    )

    app.add_error_handler(h_error)
    app.add_handler(CommandHandler("start",  h_start))
    app.add_handler(CommandHandler("admin",  h_admin))
    app.add_handler(CommandHandler("cancel", h_cancel))
    app.add_handler(CallbackQueryHandler(h_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, h_message))

    log.info("Polling started ✓")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
