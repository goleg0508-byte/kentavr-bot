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

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("bot")

# ── Config ──────────────────────────────────────────────────────────────────────
TOKEN    = os.getenv("BOT_TOKEN", "").strip()
DB_URL   = os.getenv("DATABASE_URL", "").strip()
ADMINS   = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
DB_FILE  = "bot.db"
PORT     = int(os.getenv("PORT", "8080"))

IMG_URLS = {
    "main":    "https://i.postimg.cc/RFsmw06x/Chat-GPT-Image-4-iun-2026-g-06-12-26.png",
    "buyer":   "https://i.postimg.cc/wTSw7dBP/Chat-GPT-Image-4-iun-2026-g-06-35-43.png",
    "seller":  "https://i.postimg.cc/TwFDCfFH/IMG-20260604-035610-329.png",
    "partner": "https://i.postimg.cc/fT5gqd27/Chat-GPT-Image-4-iun-2026-g-03-57-21.png",
}

TEXTS = {
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
        "Реальная внутренняя экономика — ценность растёт вместе с товарооборотом."
    ),
}

NAMES = {
    "main":    "🏠 Главный экран",
    "buyer":   "🛒 Покупатель",
    "seller":  "🏪 Продавец",
    "partner": "💎 Партнёр / ТТК",
}

# Кэш картинок url→bytes. Ключевое: в callback НИКОГДА не ждём загрузки,
# только читаем из кэша. Загрузка — только в фоне.
IMG_CACHE: dict = {}

# ── DB ──────────────────────────────────────────────────────────────────────────
PG = None
USE_PG = False


async def db_init():
    global PG, USE_PG
    if DB_URL:
        try:
            import asyncpg
            PG = await asyncio.wait_for(
                asyncpg.create_pool(DB_URL, min_size=1, max_size=5), timeout=8
            )
            async with PG.acquire() as c:
                await c.execute("CREATE TABLE IF NOT EXISTS stats(k TEXT PRIMARY KEY, v BIGINT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS users(uid BIGINT PRIMARY KEY, fs TIMESTAMPTZ DEFAULT NOW(), ls TIMESTAMPTZ DEFAULT NOW(), n INT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS ct(sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
                for k in ("starts","buyer","seller","partner"):
                    await c.execute("INSERT INTO stats(k,v) VALUES($1,0) ON CONFLICT DO NOTHING", k)
            USE_PG = True
            log.info("DB: PostgreSQL ✓")
            return
        except Exception as e:
            log.warning(f"DB: PostgreSQL fail: {e}")
            PG = None
    import aiosqlite
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS stats(k TEXT PRIMARY KEY, v INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS users(uid INTEGER PRIMARY KEY, fs TEXT, ls TEXT, n INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS ct(sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
        for k in ("starts","buyer","seller","partner"):
            await db.execute("INSERT OR IGNORE INTO stats(k,v) VALUES(?,0)", (k,))
        await db.commit()
    log.info("DB: SQLite ✓")


async def db_inc(k: str):
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                await c.execute("INSERT INTO stats(k,v) VALUES($1,1) ON CONFLICT(k) DO UPDATE SET v=stats.v+1", k)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("INSERT OR IGNORE INTO stats(k,v) VALUES(?,0)", (k,))
                await db.execute("UPDATE stats SET v=v+1 WHERE k=?", (k,))
                await db.commit()
    except Exception as e:
        log.warning(f"db_inc: {e}")


async def db_touch(uid: int):
    try:
        now = datetime.utcnow().isoformat()
        if USE_PG and PG:
            async with PG.acquire() as c:
                await c.execute("INSERT INTO users(uid) VALUES($1) ON CONFLICT(uid) DO UPDATE SET ls=NOW(),n=users.n+1", uid)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_FILE) as db:
                r = await (await db.execute("SELECT uid FROM users WHERE uid=?", (uid,))).fetchone()
                if r:
                    await db.execute("UPDATE users SET ls=?,n=n+1 WHERE uid=?", (now, uid))
                else:
                    await db.execute("INSERT INTO users(uid,fs,ls,n) VALUES(?,?,?,1)", (uid,now,now))
                await db.commit()
    except Exception as e:
        log.warning(f"db_touch: {e}")


async def db_stats():
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                rows = await c.fetch("SELECT k,v FROM stats")
                s = {r["k"]: r["v"] for r in rows}
                s["users"]   = await c.fetchval("SELECT COUNT(*) FROM users") or 0
                s["today"]   = await c.fetchval("SELECT COUNT(*) FROM users WHERE ls::date=CURRENT_DATE") or 0
                s["actions"] = await c.fetchval("SELECT SUM(n) FROM users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_FILE) as db:
                s = {r[0]: r[1] for r in await (await db.execute("SELECT k,v FROM stats")).fetchall()}
                s["users"]   = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0] or 0
                s["today"]   = (await (await db.execute("SELECT COUNT(*) FROM users WHERE date(ls)=date('now')")).fetchone())[0] or 0
                r = await (await db.execute("SELECT SUM(n) FROM users")).fetchone()
                s["actions"] = r[0] if r and r[0] else 0
        return s
    except Exception as e:
        log.warning(f"db_stats: {e}")
        return {}


async def db_uids():
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                return [r["uid"] for r in await c.fetch("SELECT uid FROM users")]
        import aiosqlite
        async with aiosqlite.connect(DB_FILE) as db:
            return [r[0] for r in await (await db.execute("SELECT uid FROM users")).fetchall()]
    except Exception:
        return []


async def db_get_ct(sk: str):
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                r = await c.fetchrow("SELECT txt,img FROM ct WHERE sk=$1", sk)
                return (r["txt"], r["img"]) if r else (None, None)
        import aiosqlite
        async with aiosqlite.connect(DB_FILE) as db:
            r = await (await db.execute("SELECT txt,img FROM ct WHERE sk=?", (sk,))).fetchone()
            return (r[0], r[1]) if r else (None, None)
    except Exception:
        return (None, None)


async def db_set_txt(sk: str, txt: str):
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                await c.execute("INSERT INTO ct(sk,txt) VALUES($1,$2) ON CONFLICT(sk) DO UPDATE SET txt=$2", sk, txt)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("INSERT OR IGNORE INTO ct(sk) VALUES(?)", (sk,))
                await db.execute("UPDATE ct SET txt=? WHERE sk=?", (txt, sk))
                await db.commit()
    except Exception as e:
        log.warning(f"db_set_txt: {e}")


async def db_reset_txt(sk: str):
    try:
        if USE_PG and PG:
            async with PG.acquire() as c:
                await c.execute("UPDATE ct SET txt=NULL WHERE sk=$1", sk)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("UPDATE ct SET txt=NULL WHERE sk=?", (sk,))
                await db.commit()
    except Exception as e:
        log.warning(f"db_reset_txt: {e}")


# ── Images ───────────────────────────────────────────────────────────────────────

def _fetch_sync(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


async def img_preload():
    """Фоновая загрузка всех картинок. Стартует после polling."""
    await asyncio.sleep(1)
    log.info("Preloading images...")
    for key, url in IMG_URLS.items():
        if url not in IMG_CACHE:
            try:
                data = await asyncio.to_thread(_fetch_sync, url)
                IMG_CACHE[url] = data
                log.info(f"  ✓ {key} {len(data)//1024}KB")
            except Exception as e:
                log.warning(f"  ✗ {key}: {e}")
    log.info(f"Preload done: {len(IMG_CACHE)}/{len(IMG_URLS)}")


# ── Keyboards ────────────────────────────────────────────────────────────────────

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Я покупатель",         callback_data="go:buyer")],
        [InlineKeyboardButton("🏪 Я продавец",           callback_data="go:seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром", callback_data="go:partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("📄 Коммерческое предложение",
                              web_app=WebAppInfo(url="https://kentavrmarket.shop"))],
    ])


def kb_back():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])


def kb_admin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",    callback_data="adm:stats"),
         InlineKeyboardButton("👥 Пользователи",  callback_data="adm:users")],
        [InlineKeyboardButton("📢 Рассылка",      callback_data="adm:broadcast"),
         InlineKeyboardButton("📥 Экспорт CSV",   callback_data="adm:export")],
        [InlineKeyboardButton("✏️ Тексты экранов", callback_data="adm:texts")],
        [InlineKeyboardButton("🏠 На главную",    callback_data="go:main")],
    ])


# ── Screen sender ────────────────────────────────────────────────────────────────

async def send_screen(bot, chat_id: int, key: str):
    """
    Отправляет экран. Картинку берёт ТОЛЬКО из кэша (мгновенно).
    Если нет в кэше — отправляет текст сразу, не ждёт загрузки.
    """
    custom_txt, custom_img = await db_get_ct(key)
    text   = custom_txt or TEXTS.get(key, TEXTS["main"])
    markup = kb_main() if key == "main" else kb_back()

    # Картинка только из кэша — без ожидания загрузки
    img_url   = custom_img or IMG_URLS.get(key)
    img_bytes = IMG_CACHE.get(img_url) if img_url else None

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

    # Текст — всегда работает
    await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, parse_mode="HTML")


async def send_admin(bot, chat_id: int, mid: int = None):
    s = await db_stats()
    t = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{s.get('users',0)}</b>  📅 Сегодня: <b>{s.get('today',0)}</b>\n"
        f"🎯 Действий: <b>{s.get('actions',0)}</b>\n\n<i>Выбери действие:</i>"
    )
    if mid:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=mid, text=t, reply_markup=kb_admin(), parse_mode="HTML")
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=t, reply_markup=kb_admin(), parse_mode="HTML")


# ── Handlers ─────────────────────────────────────────────────────────────────────

async def on_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log.info(f"/start uid={uid}")
    ctx.user_data.clear()
    asyncio.create_task(db_inc("starts"))
    asyncio.create_task(db_touch(uid))
    await send_screen(ctx.bot, update.effective_chat.id, "main")


async def on_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMINS:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    ctx.user_data.clear()
    await send_admin(ctx.bot, update.effective_chat.id)


async def on_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ Отменено.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]]),
    )


async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = update.effective_user.id
    cid  = update.effective_chat.id
    mid  = q.message.message_id
    data = q.data
    bot  = ctx.bot

    log.info(f"[CB] uid={uid} data={data!r}")
    await q.answer()  # убрать spinner немедленно

    # ── Навигация — мгновенно ─────────────────────────────────────────────────
    if data.startswith("go:"):
        key = data[3:]
        ctx.user_data.clear()

        # DB в фоне — не блокируем показ экрана
        if key in ("buyer", "seller", "partner"):
            asyncio.create_task(db_inc(key))

        # Удалить старое сообщение
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
        except Exception:
            pass

        # Показать новый экран — мгновенно (картинка из кэша или текст)
        await send_screen(bot, cid, key)
        return

    # ── Только для админов ───────────────────────────────────────────────────
    if uid not in ADMINS:
        return

    async def ed(text: str, kb: InlineKeyboardMarkup):
        try:
            await bot.edit_message_text(chat_id=cid, message_id=mid, text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await bot.send_message(chat_id=cid, text=text, reply_markup=kb, parse_mode="HTML")

    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")]])

    if data == "adm:panel":
        ctx.user_data.clear()
        await send_admin(bot, cid, mid)

    elif data == "adm:stats":
        s = await db_stats()
        await ed(
            "📊 <b>СТАТИСТИКА</b>\n━━━━━━━━━━━━━━━━\n\n"
            f"👥 Пользователей: <code>{s.get('users',0)}</code>\n"
            f"📅 Активных сегодня: <code>{s.get('today',0)}</code>\n"
            f"🎯 Действий: <code>{s.get('actions',0)}</code>\n\n"
            f"▶️ /start: <code>{s.get('starts',0)}</code>\n"
            f"🛒 Покупатель: <code>{s.get('buyer',0)}</code>\n"
            f"🏪 Продавец: <code>{s.get('seller',0)}</code>\n"
            f"💎 Партнёр: <code>{s.get('partner',0)}</code>",
            back,
        )

    elif data == "adm:users":
        uids = await db_uids()
        t = f"👥 <b>Пользователи</b> — {len(uids)} чел.\n\n"
        for i, u in enumerate(uids[-20:][::-1], 1):
            t += f"{i}. <code>{u}</code>\n"
        await ed(t, back)

    elif data == "adm:export":
        uids = await db_uids()
        s    = await db_stats()
        buf  = io.StringIO()
        w    = csv.writer(buf)
        w.writerow(["Метрика", "Значение"])
        for k, l in [("users","Пользователей"),("today","Сегодня"),("actions","Действий"),
                     ("starts","/start"),("buyer","Покупатель"),("seller","Продавец"),("partner","Партнёр")]:
            w.writerow([l, s.get(k, 0)])
        w.writerow([]); w.writerow(["user_id"])
        for u in uids: w.writerow([u])
        buf.seek(0)
        await bot.send_document(
            chat_id=cid,
            document=io.BytesIO(buf.getvalue().encode()),
            filename=f"export_{datetime.now():%Y%m%d_%H%M}.csv",
            caption="📊 Экспорт",
        )

    elif data == "adm:broadcast":
        uids = await db_uids()
        ctx.user_data["state"] = "broadcast"
        await ed(
            f"📢 <b>Рассылка</b>\n\nАудитория: <b>{len(uids)} чел.</b>\n\nОтправь текст. /cancel — отмена.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]]),
        )

    elif data == "adm:texts":
        rows = [[InlineKeyboardButton(n, callback_data=f"adm:t:{k}")] for k, n in NAMES.items()]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        await ed("✏️ <b>Тексты экранов</b>\n\nВыбери экран:", InlineKeyboardMarkup(rows))

    elif data.startswith("adm:t:"):
        sk = data[6:]
        ctx.user_data.update({"state": "edit_text", "sk": sk})
        ct, _ = await db_get_ct(sk)
        cur   = ct or TEXTS.get(sk, "")
        prev  = cur[:400] + ("…" if len(cur) > 400 else "")
        rows  = []
        if ct:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:r:{sk}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:texts")])
        await ed(
            f"✏️ <b>{NAMES.get(sk,sk)}</b> ({'изменён' if ct else 'оригинал'})\n\n"
            f"<b>Сейчас:</b>\n<blockquote>{prev}</blockquote>\n\nОтправь новый текст. /cancel — отмена.",
            InlineKeyboardMarkup(rows),
        )

    elif data.startswith("adm:r:"):
        sk = data[6:]
        await db_reset_txt(sk)
        ctx.user_data.clear()
        await ed(f"🔄 Текст «{NAMES.get(sk,sk)}» сброшен.", back)


async def on_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = ctx.user_data.get("state")

    if uid not in ADMINS or not state:
        return

    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])

    if state == "broadcast":
        uids = await db_uids()
        msg  = await update.message.reply_text(f"⏳ Рассылаю {len(uids)} пользователям…")
        ok = fail = 0
        for i, u in enumerate(uids):
            try:
                await ctx.bot.send_message(u, text, parse_mode="HTML")
                ok += 1
            except Exception:
                fail += 1
            if (i + 1) % 30 == 0:
                await asyncio.sleep(1)
        ctx.user_data.clear()
        await msg.edit_text(f"✅ Доставлено: <b>{ok}</b>  ❌ Ошибок: <b>{fail}</b>", reply_markup=back, parse_mode="HTML")

    elif state == "edit_text":
        sk = ctx.user_data.get("sk")
        if sk:
            await db_set_txt(sk, text)
            ctx.user_data.clear()
            await update.message.reply_text(
                f"✅ Текст «{NAMES.get(sk,sk)}» обновлён!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:texts")],
                    [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
                ]),
                parse_mode="HTML",
            )


async def on_err(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Ошибка: {ctx.error}", exc_info=ctx.error)


# ── Startup ──────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    # 1. БД — быстро (< 2 сек)
    try:
        await db_init()
    except Exception as e:
        log.error(f"db_init: {e}")

    # 2. Картинки — в фоне, НЕ блокируют старт бота
    asyncio.create_task(img_preload())


# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("KENTAVR MARKET Bot")
    log.info(f"TOKEN:   {'✓' if TOKEN else '✗ НЕ ЗАДАН'}")
    log.info(f"ADMINS:  {ADMINS}")
    log.info(f"DB_URL:  {'✓ PostgreSQL' if DB_URL else 'SQLite'}")
    log.info("=" * 50)

    if not TOKEN:
        raise SystemExit("BOT_TOKEN не задан в Railway Variables!")

    # Health check
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", PORT), type("H", (BaseHTTPRequestHandler,), {
            "do_GET": lambda s: (s.send_response(200), s.end_headers(), s.wfile.write(b"OK")),
            "log_message": lambda *a: None,
        })).serve_forever(),
        daemon=True,
    ).start()

    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_error_handler(on_err)
    app.add_handler(CommandHandler("start",  on_start))
    app.add_handler(CommandHandler("admin",  on_admin))
    app.add_handler(CommandHandler("cancel", on_cancel))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_msg))

    log.info("Polling ✓")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
