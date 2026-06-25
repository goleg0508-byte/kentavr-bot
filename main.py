import os
import logging
import asyncio
import threading
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
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
DB_PATH      = "kentavr.db"

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

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

db_pool      = None
USE_POSTGRES = False


# ── Health check ───────────────────────────────────────────────────────────────

class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *a): pass

def _health():
    HTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), _H).serve_forever()


# ── Database ───────────────────────────────────────────────────────────────────

async def init_db():
    global db_pool, USE_POSTGRES
    if DATABASE_URL:
        try:
            import asyncpg
            db_pool = await asyncpg.create_pool(DATABASE_URL)
            async with db_pool.acquire() as c:
                await c.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value BIGINT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS users (uid BIGINT PRIMARY KEY, first_seen TIMESTAMP DEFAULT NOW(), last_seen TIMESTAMP DEFAULT NOW(), cnt INT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS screen_content (sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
                for k in ("starts","buyer","seller","partner"):
                    await c.execute("INSERT INTO stats(key,value) VALUES($1,0) ON CONFLICT DO NOTHING", k)
            USE_POSTGRES = True
            logger.info("DB: PostgreSQL OK")
            return
        except Exception as e:
            logger.warning(f"DB PG failed: {e}, using SQLite")

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS users (uid INTEGER PRIMARY KEY, first_seen TEXT, last_seen TEXT, cnt INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS screen_content (sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
        for k in ("starts","buyer","seller","partner"):
            await db.execute("INSERT OR IGNORE INTO stats(key,value) VALUES(?,0)", (k,))
        await db.commit()
    logger.info("DB: SQLite OK")


async def _inc(key: str):
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                await c.execute("INSERT INTO stats(key,value) VALUES($1,1) ON CONFLICT(key) DO UPDATE SET value=stats.value+1", key)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO stats(key,value) VALUES(?,0)", (key,))
                await db.execute("UPDATE stats SET value=value+1 WHERE key=?", (key,))
                await db.commit()
    except Exception as e:
        logger.warning(f"_inc({key}): {e}")


async def _touch(uid: int):
    try:
        now = datetime.utcnow().isoformat()
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                await c.execute(
                    "INSERT INTO users(uid,first_seen,last_seen,cnt) VALUES($1,NOW(),NOW(),1) "
                    "ON CONFLICT(uid) DO UPDATE SET last_seen=NOW(), cnt=users.cnt+1", uid)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                row = (await (await db.execute("SELECT uid FROM users WHERE uid=?", (uid,))).fetchone())
                if row:
                    await db.execute("UPDATE users SET last_seen=?,cnt=cnt+1 WHERE uid=?", (now, uid))
                else:
                    await db.execute("INSERT INTO users(uid,first_seen,last_seen,cnt) VALUES(?,?,?,1)", (uid,now,now))
                await db.commit()
    except Exception as e:
        logger.warning(f"_touch: {e}")


async def _stats() -> dict:
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                rows = await c.fetch("SELECT key,value FROM stats")
                s = {r['key']: r['value'] for r in rows}
                s['users']  = await c.fetchval("SELECT COUNT(*) FROM users") or 0
                s['today']  = await c.fetchval("SELECT COUNT(*) FROM users WHERE last_seen::date=CURRENT_DATE") or 0
                s['actions']= await c.fetchval("SELECT SUM(cnt) FROM users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                rows = await (await db.execute("SELECT key,value FROM stats")).fetchall()
                s = {r[0]: r[1] for r in rows}
                s['users']  = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0] or 0
                s['today']  = (await (await db.execute("SELECT COUNT(*) FROM users WHERE date(last_seen)=date('now')")).fetchone())[0] or 0
                r = await (await db.execute("SELECT SUM(cnt) FROM users")).fetchone()
                s['actions']= (r[0] if r and r[0] else 0)
        return s
    except Exception as e:
        logger.warning(f"_stats: {e}")
        return {}


async def _all_uids() -> list:
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                return [r['uid'] for r in await c.fetch("SELECT uid FROM users")]
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            return [r[0] for r in await (await db.execute("SELECT uid FROM users")).fetchall()]
    except Exception:
        return []


async def _get_ct(sk: str):
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                r = await c.fetchrow("SELECT txt,img FROM screen_content WHERE sk=$1", sk)
                return (r['txt'], r['img']) if r else (None, None)
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            r = await (await db.execute("SELECT txt,img FROM screen_content WHERE sk=?", (sk,))).fetchone()
            return (r[0], r[1]) if r else (None, None)
    except Exception:
        return (None, None)


async def _set_ct(sk: str, txt: str = None, img: str = None):
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                if txt is not None:
                    await c.execute("INSERT INTO screen_content(sk,txt) VALUES($1,$2) ON CONFLICT(sk) DO UPDATE SET txt=$2", sk, txt)
                if img is not None:
                    await c.execute("INSERT INTO screen_content(sk,img) VALUES($1,$2) ON CONFLICT(sk) DO UPDATE SET img=$2", sk, img)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                if txt is not None:
                    await db.execute("INSERT OR IGNORE INTO screen_content(sk) VALUES(?)", (sk,))
                    await db.execute("UPDATE screen_content SET txt=? WHERE sk=?", (txt, sk))
                if img is not None:
                    await db.execute("INSERT OR IGNORE INTO screen_content(sk) VALUES(?)", (sk,))
                    await db.execute("UPDATE screen_content SET img=? WHERE sk=?", (img, sk))
                await db.commit()
    except Exception as e:
        logger.warning(f"_set_ct: {e}")


async def _reset_ct(sk: str, what: str):
    col = "txt" if what == "txt" else "img"
    try:
        if USE_POSTGRES and db_pool:
            async with db_pool.acquire() as c:
                await c.execute(f"UPDATE screen_content SET {col}=NULL WHERE sk=$1", sk)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(f"UPDATE screen_content SET {col}=NULL WHERE sk=?", (sk,))
                await db.commit()
    except Exception as e:
        logger.warning(f"_reset_ct: {e}")


def _is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


# ── Screen content ─────────────────────────────────────────────────────────────

def _screen_main():
    text = (
        "👋 <b>Привет!</b>\n\n"
        "Добро пожаловать в <b>KENTAVR MARKET</b> — социальный маркетплейс.\n\n"
        "Все подробности о платформе есть в нашем лендинге ниже.\n\n"
        "<i>Кем ты являешься?</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Я покупатель",         callback_data="go:buyer")],
        [InlineKeyboardButton("🏪 Я продавец",           callback_data="go:seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром", callback_data="go:partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("📄 Коммерческое предложение", web_app=WebAppInfo(url="https://kentavrmarket.shop"))],
    ])
    return text, kb


def _screen_buyer():
    text = (
        "🛒 <b>Для покупателей</b>\n\n"
        "Покупай у проверенных участников сообщества и получай "
        "<b>кэшбэк в ТТК</b> за каждую покупку.\n\n"
        "Чем активнее — тем больше возможностей открывается."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])
    return text, kb


def _screen_seller():
    text = (
        "🏪 <b>Для продавцов</b>\n\n"
        "Размести товары, услуги или экспертизу и получи доступ "
        "к активной аудитории.\n\n"
        "Здесь строят <b>долгосрочные отношения</b>, а не разовые сделки."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])
    return text, kb


def _screen_partner():
    text = (
        "💎 <b>Партнёр / ТТК</b>\n\n"
        "Участвуй в развитии платформы и получай "
        "<b>Торговый Токен KENTAVR (ТТК)</b>.\n\n"
        "Его ценность растёт вместе с товарооборотом сообщества — "
        "реальная внутренняя экономика, не биржевая крипта."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])
    return text, kb


SCREENS = {
    "main":    _screen_main,
    "buyer":   _screen_buyer,
    "seller":  _screen_seller,
    "partner": _screen_partner,
}


async def _send_screen(bot, chat_id: int, key: str):
    fn = SCREENS.get(key, _screen_main)
    default_text, markup = fn()
    custom_txt, custom_img = await _get_ct(key)
    text  = custom_txt or default_text
    image = custom_img or SCREEN_IMAGES.get(key)
    if image:
        try:
            await bot.send_photo(chat_id=chat_id, photo=image,
                                 caption=text, reply_markup=markup, parse_mode="HTML")
            return
        except Exception as e:
            logger.warning(f"send_photo({key}): {e}")
    await bot.send_message(chat_id=chat_id, text=text,
                           reply_markup=markup, parse_mode="HTML")


# ── Admin panel ────────────────────────────────────────────────────────────────

async def _admin_panel(bot, chat_id: int, mid: int = None):
    s = await _stats()
    text = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{s.get('users',0)}</b>   "
        f"📅 Сегодня: <b>{s.get('today',0)}</b>\n"
        f"🎯 Всего действий: <b>{s.get('actions',0)}</b>\n\n"
        "<i>Выбери действие:</i>"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",        callback_data="adm:stats"),
         InlineKeyboardButton("👥 Пользователи",      callback_data="adm:users")],
        [InlineKeyboardButton("📢 Рассылка",          callback_data="adm:broadcast"),
         InlineKeyboardButton("📥 Экспорт CSV",       callback_data="adm:export")],
        [InlineKeyboardButton("✏️ Тексты экранов",    callback_data="adm:texts")],
        [InlineKeyboardButton("🖼 Картинки экранов",   callback_data="adm:images")],
        [InlineKeyboardButton("🏠 На главную",         callback_data="go:main")],
    ])
    if mid:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=mid,
                                        text=text, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML")


# ── Handlers ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    logger.info(f"[start] uid={uid}")
    context.user_data.clear()
    await asyncio.gather(_inc("starts"), _touch(uid))
    await _send_screen(context.bot, update.effective_chat.id, "main")


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not _is_admin(uid):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    context.user_data.clear()
    await _admin_panel(context.bot, update.effective_chat.id)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
    await update.message.reply_text("❌ Отменено.", reply_markup=kb)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = update.effective_user.id
    cid = update.effective_chat.id
    mid = q.message.message_id
    bot = context.bot
    data = q.data

    logger.info(f"[cb] uid={uid} data={data}")
    await q.answer()

    # ── Screen navigation ──────────────────────────────────────────────────────
    if data.startswith("go:"):
        key = data[3:]
        context.user_data.clear()
        if key in ("buyer", "seller", "partner"):
            await _inc(key)
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
        except Exception:
            pass
        await _send_screen(bot, cid, key)
        return

    # ── Admin ──────────────────────────────────────────────────────────────────
    if not _is_admin(uid):
        return

    if data == "adm:panel":
        context.user_data.clear()
        await _admin_panel(bot, cid, mid)

    elif data == "adm:stats":
        s = await _stats()
        t = (
            "📊 <b>СТАТИСТИКА</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Пользователей: <code>{s.get('users',0)}</code>\n"
            f"📅 Активных сегодня: <code>{s.get('today',0)}</code>\n"
            f"🎯 Всего действий: <code>{s.get('actions',0)}</code>\n\n"
            f"▶️ /start: <code>{s.get('starts',0)}</code>\n"
            f"🛒 Покупатель: <code>{s.get('buyer',0)}</code>\n"
            f"🏪 Продавец: <code>{s.get('seller',0)}</code>\n"
            f"💎 Партнёр: <code>{s.get('partner',0)}</code>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=t, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=t, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:users":
        uids = await _all_uids()
        t = f"👥 <b>Пользователи</b> — всего: <code>{len(uids)}</code>\n\n"
        for i, u in enumerate(uids[-20:][::-1], 1):
            t += f"{i}. <code>{u}</code>\n"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=t, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=t, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:export":
        uids = await _all_uids()
        s    = await _stats()
        buf  = io.StringIO()
        w    = csv.writer(buf)
        w.writerow(["Метрика", "Значение"])
        for k, lbl in [("users","Пользователей"),("today","Сегодня"),("actions","Действий"),
                        ("starts","/start"),("buyer","Покупатель"),("seller","Продавец"),("partner","Партнёр")]:
            w.writerow([lbl, s.get(k, 0)])
        w.writerow([]); w.writerow(["user_id"])
        for u in uids: w.writerow([u])
        buf.seek(0)
        await bot.send_document(chat_id=cid,
            document=io.BytesIO(buf.getvalue().encode()),
            filename=f"kentavr_{datetime.now():%Y%m%d_%H%M}.csv",
            caption="📊 Экспорт KENTAVR MARKET")

    elif data == "adm:broadcast":
        uids = await _all_uids()
        context.user_data['state'] = 'broadcast'
        t  = f"📢 <b>Рассылка</b>\n\nАудитория: <b>{len(uids)} чел.</b>\n\nОтправь текст сообщения. /cancel — отмена."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=t, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=t, reply_markup=kb, parse_mode="HTML")

    elif data == "adm:texts":
        context.user_data['state'] = 'texts_menu'
        rows = [[InlineKeyboardButton(name, callback_data=f"adm:pick_t:{k}")]
                for k, name in SCREEN_NAMES.items()]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        kb = InlineKeyboardMarkup(rows)
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid,
                text="✏️ <b>Тексты</b>\n\nВыбери экран:", reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid,
                text="✏️ <b>Тексты</b>\n\nВыбери экран:", reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:pick_t:"):
        sk = data[len("adm:pick_t:"):]
        context.user_data.update({'state': 'edit_text', 'sk': sk})
        ct, _ = await _get_ct(sk)
        def_t, _ = SCREENS.get(sk, _screen_main)()
        cur   = ct or def_t
        prev  = cur[:400] + ("…" if len(cur) > 400 else "")
        lbl   = "изменён" if ct else "оригинал"
        t = (f"✏️ <b>{SCREEN_NAMES.get(sk, sk)}</b> ({lbl})\n\n"
             f"<b>Текущий текст:</b>\n<blockquote>{prev}</blockquote>\n\n"
             "Отправь новый текст. /cancel — отмена.")
        rows = []
        if ct:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:rst_t:{sk}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:texts")])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=t,
                reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=t,
                reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")

    elif data.startswith("adm:rst_t:"):
        sk = data[len("adm:rst_t:"):]
        await _reset_ct(sk, "txt")
        context.user_data.clear()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid,
                text=f"🔄 Текст <b>{SCREEN_NAMES.get(sk,sk)}</b> сброшен.",
                reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid,
                text=f"🔄 Текст <b>{SCREEN_NAMES.get(sk,sk)}</b> сброшен.",
                reply_markup=kb, parse_mode="HTML")

    elif data == "adm:images":
        context.user_data['state'] = 'images_menu'
        rows = [[InlineKeyboardButton(name, callback_data=f"adm:pick_i:{k}")]
                for k, name in SCREEN_NAMES.items()]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        kb = InlineKeyboardMarkup(rows)
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid,
                text="🖼 <b>Картинки</b>\n\nВыбери экран:", reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid,
                text="🖼 <b>Картинки</b>\n\nВыбери экран:", reply_markup=kb, parse_mode="HTML")

    elif data.startswith("adm:pick_i:"):
        sk = data[len("adm:pick_i:"):]
        context.user_data.update({'state': 'edit_image', 'sk': sk})
        _, ci = await _get_ct(sk)
        cur   = ci or SCREEN_IMAGES.get(sk, "не задана")
        lbl   = "изменена" if ci else "оригинал"
        t = (f"🖼 <b>{SCREEN_NAMES.get(sk, sk)}</b> ({lbl})\n\n"
             f"<b>Текущая ссылка:</b>\n<code>{cur}</code>\n\n"
             "Отправь новую ссылку (https://…). /cancel — отмена.")
        rows = []
        if ci:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:rst_i:{sk}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:images")])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=t,
                reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=t,
                reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")

    elif data.startswith("adm:rst_i:"):
        sk = data[len("adm:rst_i:"):]
        await _reset_ct(sk, "img")
        context.user_data.clear()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid,
                text=f"🔄 Картинка <b>{SCREEN_NAMES.get(sk,sk)}</b> сброшена.",
                reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid,
                text=f"🔄 Картинка <b>{SCREEN_NAMES.get(sk,sk)}</b> сброшена.",
                reply_markup=kb, parse_mode="HTML")


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = context.user_data.get('state')

    if not _is_admin(uid) or not state:
        return

    sk = context.user_data.get('sk')

    if state == 'broadcast':
        uids   = await _all_uids()
        sent   = failed = 0
        msg    = await update.message.reply_text(f"⏳ Отправляю {len(uids)} пользователям…")
        for i, u in enumerate(uids):
            try:
                await context.bot.send_message(u, text, parse_mode="HTML")
                sent += 1
            except Exception:
                failed += 1
            if (i + 1) % 30 == 0:
                await asyncio.sleep(1)
        context.user_data.clear()
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])
        await msg.edit_text(
            f"✅ Доставлено: <b>{sent}</b>\n❌ Ошибок: <b>{failed}</b>",
            reply_markup=kb, parse_mode="HTML")

    elif state == 'edit_text' and sk:
        await _set_ct(sk, txt=text)
        context.user_data.clear()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:texts")],
            [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
        ])
        await update.message.reply_text(
            f"✅ Текст <b>{SCREEN_NAMES.get(sk,sk)}</b> обновлён!",
            reply_markup=kb, parse_mode="HTML")

    elif state == 'edit_image' and sk:
        url = text.strip()
        if not url.startswith("http"):
            await update.message.reply_text("❌ Ссылка должна начинаться с https://")
            return
        await _set_ct(sk, img=url)
        context.user_data.clear()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🖼 Изменить ещё", callback_data="adm:images")],
            [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
        ])
        await update.message.reply_text(
            f"✅ Картинка <b>{SCREEN_NAMES.get(sk,sk)}</b> обновлена!",
            reply_markup=kb, parse_mode="HTML")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}", exc_info=context.error)


# ── post_init + main ───────────────────────────────────────────────────────────

async def _post_init(app):
    await init_db()


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не установлен!")
    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS не установлен — admin-панель недоступна")

    threading.Thread(target=_health, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).build()
    app.add_error_handler(on_error)
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("admin",  cmd_admin))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    logger.info("🚀 KENTAVR MARKET бот запущен")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
