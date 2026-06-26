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

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bot")

# ── Конфиг ──────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
DB_PATH      = "bot.db"

IMAGES = {
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
        "Реальная внутренняя экономика — ценность растёт "
        "вместе с товарооборотом."
    ),
}

NAMES = {
    "main":    "🏠 Главный экран",
    "buyer":   "🛒 Покупатель",
    "seller":  "🏪 Продавец",
    "partner": "💎 Партнёр / ТТК",
}

# Кэш картинок: url → bytes (заполняется лениво, в фоне)
_cache: dict = {}

# ── БД ──────────────────────────────────────────────────────────────────────────
_pool   = None
_use_pg = False


async def _db_init():
    global _pool, _use_pg
    if DATABASE_URL:
        try:
            import asyncpg
            _pool = await asyncio.wait_for(
                asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5),
                timeout=10,
            )
            async with _pool.acquire() as c:
                await c.execute("CREATE TABLE IF NOT EXISTS stats (k TEXT PRIMARY KEY, v BIGINT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS users (uid BIGINT PRIMARY KEY, fs TIMESTAMPTZ DEFAULT NOW(), ls TIMESTAMPTZ DEFAULT NOW(), n INT DEFAULT 0)")
                await c.execute("CREATE TABLE IF NOT EXISTS ct (sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
                for k in ("starts", "buyer", "seller", "partner"):
                    await c.execute("INSERT INTO stats(k,v) VALUES($1,0) ON CONFLICT DO NOTHING", k)
            _use_pg = True
            log.info("DB: PostgreSQL ✓")
            return
        except Exception as e:
            log.warning(f"DB: PostgreSQL failed ({e}), using SQLite")
            _pool = None

    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS stats (k TEXT PRIMARY KEY, v INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS users (uid INTEGER PRIMARY KEY, fs TEXT, ls TEXT, n INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS ct (sk TEXT PRIMARY KEY, txt TEXT, img TEXT)")
        for k in ("starts", "buyer", "seller", "partner"):
            await db.execute("INSERT OR IGNORE INTO stats(k,v) VALUES(?,0)", (k,))
        await db.commit()
    log.info("DB: SQLite ✓")


async def _inc(k: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute("INSERT INTO stats(k,v) VALUES($1,1) ON CONFLICT(k) DO UPDATE SET v=stats.v+1", k)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO stats(k,v) VALUES(?,0)", (k,))
                await db.execute("UPDATE stats SET v=v+1 WHERE k=?", (k,))
                await db.commit()
    except Exception as e:
        log.warning(f"_inc: {e}")


async def _touch(uid: int):
    try:
        now = datetime.utcnow().isoformat()
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute("INSERT INTO users(uid) VALUES($1) ON CONFLICT(uid) DO UPDATE SET ls=NOW(), n=users.n+1", uid)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                r = await (await db.execute("SELECT uid FROM users WHERE uid=?", (uid,))).fetchone()
                if r:
                    await db.execute("UPDATE users SET ls=?,n=n+1 WHERE uid=?", (now, uid))
                else:
                    await db.execute("INSERT INTO users(uid,fs,ls,n) VALUES(?,?,?,1)", (uid, now, now))
                await db.commit()
    except Exception as e:
        log.warning(f"_touch: {e}")


async def _stats() -> dict:
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                rows = await c.fetch("SELECT k,v FROM stats")
                s = {r["k"]: r["v"] for r in rows}
                s["users"]   = await c.fetchval("SELECT COUNT(*) FROM users") or 0
                s["today"]   = await c.fetchval("SELECT COUNT(*) FROM users WHERE ls::date=CURRENT_DATE") or 0
                s["actions"] = await c.fetchval("SELECT SUM(n) FROM users") or 0
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                s = {r[0]: r[1] for r in await (await db.execute("SELECT k,v FROM stats")).fetchall()}
                s["users"]   = (await (await db.execute("SELECT COUNT(*) FROM users")).fetchone())[0] or 0
                s["today"]   = (await (await db.execute("SELECT COUNT(*) FROM users WHERE date(ls)=date('now')")).fetchone())[0] or 0
                r = await (await db.execute("SELECT SUM(n) FROM users")).fetchone()
                s["actions"] = r[0] if r and r[0] else 0
        return s
    except Exception as e:
        log.warning(f"_stats: {e}")
        return {}


async def _uids() -> list:
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                return [r["uid"] for r in await c.fetch("SELECT uid FROM users")]
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            return [r[0] for r in await (await db.execute("SELECT uid FROM users")).fetchall()]
    except Exception:
        return []


async def _get_ct(sk: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                r = await c.fetchrow("SELECT txt,img FROM ct WHERE sk=$1", sk)
                return (r["txt"], r["img"]) if r else (None, None)
        import aiosqlite
        async with aiosqlite.connect(DB_PATH) as db:
            r = await (await db.execute("SELECT txt,img FROM ct WHERE sk=?", (sk,))).fetchone()
            return (r[0], r[1]) if r else (None, None)
    except Exception:
        return (None, None)


async def _set_ct(sk: str, txt=None, img=None):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                if txt is not None:
                    await c.execute("INSERT INTO ct(sk,txt) VALUES($1,$2) ON CONFLICT(sk) DO UPDATE SET txt=$2", sk, txt)
                if img is not None:
                    await c.execute("INSERT INTO ct(sk,img) VALUES($1,$2) ON CONFLICT(sk) DO UPDATE SET img=$2", sk, img)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR IGNORE INTO ct(sk) VALUES(?)", (sk,))
                if txt is not None:
                    await db.execute("UPDATE ct SET txt=? WHERE sk=?", (txt, sk))
                if img is not None:
                    await db.execute("UPDATE ct SET img=? WHERE sk=?", (img, sk))
                await db.commit()
    except Exception as e:
        log.warning(f"_set_ct: {e}")


async def _rst_ct(sk: str, col: str):
    try:
        if _use_pg and _pool:
            async with _pool.acquire() as c:
                await c.execute(f"UPDATE ct SET {col}=NULL WHERE sk=$1", sk)
        else:
            import aiosqlite
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(f"UPDATE ct SET {col}=NULL WHERE sk=?", (sk,))
                await db.commit()
    except Exception as e:
        log.warning(f"_rst_ct: {e}")


# ── Картинки ─────────────────────────────────────────────────────────────────────

def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


async def _img(url: str):
    if url in _cache:
        return _cache[url]
    try:
        data = await asyncio.to_thread(_fetch, url)
        _cache[url] = data
        log.info(f"img cached {len(data)//1024}KB: {url[:55]}")
        return data
    except Exception as e:
        log.warning(f"img failed: {e}")
        return None


async def _preload_bg():
    """Фоновая загрузка картинок — не блокирует старт бота."""
    await asyncio.sleep(1)  # дать боту запустить polling
    log.info("Preloading images in background...")
    for key, url in IMAGES.items():
        await _img(url)
    log.info(f"Preload done: {len(_cache)}/{len(IMAGES)} cached")


# ── Клавиатуры ───────────────────────────────────────────────────────────────────

def _kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Я покупатель",         callback_data="go:buyer")],
        [InlineKeyboardButton("🏪 Я продавец",           callback_data="go:seller")],
        [InlineKeyboardButton("💎 Хочу стать партнёром", callback_data="go:partner")],
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("📄 Коммерческое предложение",
                              web_app=WebAppInfo(url="https://kentavrmarket.shop"))],
    ])


def _kb_back():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Перейти на платформу", url="https://shop.kentavr.world/")],
        [InlineKeyboardButton("🏠 На главную",           callback_data="go:main")],
    ])


def _kb_admin():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика",    callback_data="adm:stats"),
         InlineKeyboardButton("👥 Пользователи",  callback_data="adm:users")],
        [InlineKeyboardButton("📢 Рассылка",      callback_data="adm:broadcast"),
         InlineKeyboardButton("📥 Экспорт",       callback_data="adm:export")],
        [InlineKeyboardButton("✏️ Тексты",        callback_data="adm:texts")],
        [InlineKeyboardButton("🏠 На главную",    callback_data="go:main")],
    ])


# ── Показ экрана ─────────────────────────────────────────────────────────────────

async def show(bot, chat_id: int, key: str):
    ctxt, cimg = await _get_ct(key)
    text   = ctxt or TEXTS.get(key, TEXTS["main"])
    markup = _kb_main() if key == "main" else _kb_back()
    url    = cimg or IMAGES.get(key)

    if url:
        data = await _img(url)  # использует _cache внутри
        if data:
            try:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=io.BytesIO(data),
                    caption=text,
                    reply_markup=markup,
                    parse_mode="HTML",
                )
                return
            except Exception as e:
                log.warning(f"send_photo({key}): {e}")

    await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup, parse_mode="HTML")


async def show_admin(bot, chat_id: int, mid: int = None):
    s = await _stats()
    t = (
        "🎛 <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Пользователей: <b>{s.get('users', 0)}</b>   "
        f"📅 Сегодня: <b>{s.get('today', 0)}</b>\n"
        f"🎯 Действий: <b>{s.get('actions', 0)}</b>\n\n"
        "<i>Выбери действие:</i>"
    )
    if mid:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=mid,
                text=t, reply_markup=_kb_admin(), parse_mode="HTML")
            return
        except Exception:
            pass
    await bot.send_message(chat_id=chat_id, text=t, reply_markup=_kb_admin(), parse_mode="HTML")


# ── Health ────────────────────────────────────────────────────────────────────────

class _H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, *_): pass


# ── Хэндлеры ────────────────────────────────────────────────────────────────────

async def on_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log.info(f"/start uid={uid}")
    ctx.user_data.clear()
    await asyncio.gather(_inc("starts"), _touch(uid))
    await show(ctx.bot, update.effective_chat.id, "main")


async def on_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    ctx.user_data.clear()
    await show_admin(ctx.bot, update.effective_chat.id)


async def on_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ Отменено.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")
        ]]),
    )


async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    uid  = update.effective_user.id
    cid  = update.effective_chat.id
    mid  = q.message.message_id
    data = q.data
    bot  = ctx.bot

    log.info(f"[CB] uid={uid} data={data!r}")

    await q.answer()   # убираем "часики" сразу

    # ── Навигация ─────────────────────────────────────────────────────────────
    if data.startswith("go:"):
        key = data[3:]
        ctx.user_data.clear()
        if key in ("buyer", "seller", "partner"):
            await _inc(key)
        try:
            await bot.delete_message(chat_id=cid, message_id=mid)
        except Exception:
            pass
        await show(bot, cid, key)
        return

    # ── Только для админов ────────────────────────────────────────────────────
    if uid not in ADMIN_IDS:
        return

    async def ed(text: str, kb: InlineKeyboardMarkup):
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
        s = await _stats()
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
        uids = await _uids()
        t = f"👥 <b>Пользователи</b> — всего: <code>{len(uids)}</code>\n\n"
        for i, u in enumerate(uids[-20:][::-1], 1):
            t += f"{i}. <code>{u}</code>\n"
        await ed(t, back)

    elif data == "adm:export":
        uids = await _uids()
        s    = await _stats()
        buf  = io.StringIO()
        w    = csv.writer(buf)
        w.writerow(["Метрика", "Значение"])
        for k, lbl in [("users","Польз."),("today","Сегодня"),("actions","Действий"),
                        ("starts","/start"),("buyer","Покупатель"),
                        ("seller","Продавец"),("partner","Партнёр")]:
            w.writerow([lbl, s.get(k, 0)])
        w.writerow([]); w.writerow(["user_id"])
        for u in uids:
            w.writerow([u])
        buf.seek(0)
        await bot.send_document(
            chat_id=cid,
            document=io.BytesIO(buf.getvalue().encode()),
            filename=f"export_{datetime.now():%Y%m%d_%H%M}.csv",
            caption="📊 Экспорт KENTAVR MARKET",
        )

    elif data == "adm:broadcast":
        uids = await _uids()
        ctx.user_data["state"] = "broadcast"
        await ed(
            f"📢 <b>Рассылка</b>\n\n"
            f"Аудитория: <b>{len(uids)} чел.</b>\n\n"
            "Отправь текст. /cancel — отмена.",
            InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="adm:panel")]]),
        )

    elif data == "adm:texts":
        ctx.user_data["state"] = "texts_menu"
        rows = [[InlineKeyboardButton(n, callback_data=f"adm:t:{k}")] for k, n in NAMES.items()]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="adm:panel")])
        await ed("✏️ <b>Тексты экранов</b>\n\nВыбери экран:", InlineKeyboardMarkup(rows))

    elif data.startswith("adm:t:"):
        sk = data[6:]
        ctx.user_data.update({"state": "edit_text", "sk": sk})
        ct, _ = await _get_ct(sk)
        cur   = ct or TEXTS.get(sk, "")
        prev  = cur[:400] + ("…" if len(cur) > 400 else "")
        rows  = []
        if ct:
            rows.append([InlineKeyboardButton("🔄 Сбросить", callback_data=f"adm:rt:{sk}")])
        rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="adm:texts")])
        await ed(
            f"✏️ <b>{NAMES.get(sk, sk)}</b> ({'изменён' if ct else 'оригинал'})\n\n"
            f"<b>Сейчас:</b>\n<blockquote>{prev}</blockquote>\n\n"
            "Отправь новый текст. /cancel — отмена.",
            InlineKeyboardMarkup(rows),
        )

    elif data.startswith("adm:rt:"):
        sk = data[7:]
        await _rst_ct(sk, "txt")
        ctx.user_data.clear()
        await ed(f"🔄 Текст «{NAMES.get(sk, sk)}» сброшен.", back)


async def on_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    text  = update.message.text or ""
    state = ctx.user_data.get("state")

    if uid not in ADMIN_IDS or not state:
        return

    sk   = ctx.user_data.get("sk")
    back = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В панель", callback_data="adm:panel")]])

    if state == "broadcast":
        uids = await _uids()
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
        await msg.edit_text(
            f"✅ Доставлено: <b>{ok}</b>  ❌ Ошибок: <b>{fail}</b>",
            reply_markup=back, parse_mode="HTML")

    elif state == "edit_text" and sk:
        await _set_ct(sk, txt=text)
        ctx.user_data.clear()
        await update.message.reply_text(
            f"✅ Текст «{NAMES.get(sk, sk)}» обновлён!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Изменить ещё", callback_data="adm:texts")],
                [InlineKeyboardButton("⬅️ В панель",     callback_data="adm:panel")],
            ]),
            parse_mode="HTML",
        )


async def on_err(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {ctx.error}", exc_info=ctx.error)


# ── Инициализация ──────────────────────────────────────────────────────────────

async def post_init(app: Application):
    # Только БД — быстро (< 2 сек). Картинки грузим в фоне после старта.
    try:
        await _db_init()
    except Exception as e:
        log.error(f"db_init failed: {e}")

    # Запускаем фоновую загрузку картинок — НЕ блокируем polling
    asyncio.create_task(_preload_bg())


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("  KENTAVR MARKET bot")
    log.info(f"  BOT_TOKEN:    {'SET ✓' if BOT_TOKEN else '❌ MISSING'}")
    log.info(f"  ADMIN_IDS:    {ADMIN_IDS or 'не задан'}")
    log.info(f"  DATABASE_URL: {'SET ✓' if DATABASE_URL else 'нет (SQLite)'}")
    log.info("=" * 55)

    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан. Добавь в Railway → Variables.")

    # Health-check
    port = int(os.getenv("PORT", "8080"))
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", port), _H).serve_forever(),
        daemon=True,
    ).start()
    log.info(f"Health server: port {port}")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_error_handler(on_err)
    app.add_handler(CommandHandler("start",  on_start))
    app.add_handler(CommandHandler("admin",  on_admin))
    app.add_handler(CommandHandler("cancel", on_cancel))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_msg))

    log.info("Starting polling... ✓")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
