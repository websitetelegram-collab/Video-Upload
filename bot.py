"""
Telegram Private Video Sharing Bot
Production-ready | Firebase Firestore | python-telegram-bot
"""

import os
import json
import logging
import asyncio
import random
import string
import time
from datetime import datetime, timezone, timedelta
from functools import wraps
from typing import Optional

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden, BadRequest

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Environment ─────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
FORCE_JOIN_CHANNEL = os.environ.get("FORCE_JOIN_CHANNEL", "")

# ─── Firebase Init ───────────────────────────────────────────────────────────
_sa_json = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]
_sa_dict = json.loads(_sa_json)
cred = credentials.Certificate(_sa_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ─── Firestore Collections ───────────────────────────────────────────────────
COL_USERS      = "users"
COL_VIDEOS     = "videos"
COL_VIEWS      = "views"
COL_SETTINGS   = "settings"
COL_ANALYTICS  = "analytics"
COL_BROADCAST  = "broadcastLogs"

# ─── Conversation States ─────────────────────────────────────────────────────
(
    AWAIT_VIDEO,
    AWAIT_TITLE,
    AWAIT_DELETE_CODE,
    AWAIT_EDIT_CODE,
    AWAIT_EDIT_TITLE,
    AWAIT_BLOCK_ID,
    AWAIT_UNBLOCK_ID,
    AWAIT_BROADCAST_CONTENT,
    AWAIT_BROADCAST_CONFIRM,
) = range(9)

# ─── Rate Limiting ───────────────────────────────────────────────────────────
_rate_cache: dict[int, list[float]] = {}
RATE_LIMIT = 10
RATE_WINDOW = 60

def rate_limited(user_id: int) -> bool:
    now = time.time()
    hits = _rate_cache.get(user_id, [])
    hits = [t for t in hits if now - t < RATE_WINDOW]
    if len(hits) >= RATE_LIMIT:
        _rate_cache[user_id] = hits
        return True
    hits.append(now)
    _rate_cache[user_id] = hits
    return False

# ─── Decorators ──────────────────────────────────────────────────────────────
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if uid not in ADMIN_IDS:
            await update.message.reply_text("⛔ Admins only.")
            return ConversationHandler.END
        return await func(update, ctx)
    return wrapper

def user_rate_guard(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if rate_limited(uid):
            await update.message.reply_text("🐢 Slow down — try again shortly.")
            return
        return await func(update, ctx)
    return wrapper

# ─── Helpers ─────────────────────────────────────────────────────────────────
def generate_code(length=8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def ts_today() -> str:
    return now_utc().strftime("%Y-%m-%d")

def ts_week() -> str:
    d = now_utc()
    return f"{d.year}-W{d.isocalendar()[1]:02d}"

def ts_month() -> str:
    return now_utc().strftime("%Y-%m")

async def get_setting(key: str, default=None):
    doc = db.collection(COL_SETTINGS).document(key).get()
    return doc.to_dict().get("value", default) if doc.exists else default

async def set_setting(key: str, value):
    db.collection(COL_SETTINGS).document(key).set({"value": value})

async def is_maintenance() -> bool:
    return await get_setting("maintenance", False)

async def upsert_user(user):
    ref = db.collection(COL_USERS).document(str(user.id))
    doc = ref.get()
    data = {
        "username": user.username or "",
        "full_name": user.full_name,
        "last_seen": SERVER_TIMESTAMP,
    }
    if not doc.exists:
        data["joined_at"] = SERVER_TIMESTAMP
        data["blocked"] = False
        data["total_views"] = 0
    ref.set(data, merge=True)

async def check_force_join(user_id: int, bot) -> bool:
    if not FORCE_JOIN_CHANNEL:
        return True
    try:
        member = await bot.get_chat_member(FORCE_JOIN_CHANNEL, user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return True

async def record_view(video_id: str, user_id: int):
    today = ts_today()
    week  = ts_week()
    month = ts_month()
    batch = db.batch()

    view_ref = db.collection(COL_VIEWS).document()
    batch.set(view_ref, {
        "video_id": video_id,
        "user_id": user_id,
        "date": today,
        "week": week,
        "month": month,
        "ts": SERVER_TIMESTAMP,
    })

    vid_ref = db.collection(COL_VIDEOS).document(video_id)
    batch.update(vid_ref, {
        "views_total": firestore.Increment(1),
        f"views_daily.{today}": firestore.Increment(1),
        f"views_weekly.{week}": firestore.Increment(1),
        f"views_monthly.{month}": firestore.Increment(1),
    })

    ana_ref = db.collection(COL_ANALYTICS).document("global")
    batch.set(ana_ref, {
        "views_total": firestore.Increment(1),
        f"views_daily.{today}": firestore.Increment(1),
        f"views_weekly.{week}": firestore.Increment(1),
        f"views_monthly.{month}": firestore.Increment(1),
    }, merge=True)

    user_ref = db.collection(COL_USERS).document(str(user_id))
    batch.update(user_ref, {"total_views": firestore.Increment(1), "last_seen": SERVER_TIMESTAMP})

    batch.commit()

# ─── /start ──────────────────────────────────────────────────────────────────
@user_rate_guard
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await upsert_user(user)

    args = ctx.args
    if args:
        await handle_video_request(update, ctx, args[0])
        return

    if await is_maintenance() and user.id not in ADMIN_IDS:
        await update.message.reply_text("🔧 Bot is under maintenance. Please check back later.")
        return

    if user.id in ADMIN_IDS:
        await show_admin_menu(update)
    else:
        await update.message.reply_text("👋 Welcome!\n\nSend me a video link to watch your content.")

async def handle_video_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, code: str):
    user = update.effective_user

    if await is_maintenance() and user.id not in ADMIN_IDS:
        await update.message.reply_text("🔧 Bot is under maintenance.")
        return

    u_doc = db.collection(COL_USERS).document(str(user.id)).get()
    if u_doc.exists and u_doc.to_dict().get("blocked"):
        await update.message.reply_text("⛔ You have been blocked from using this bot.")
        return

    if not await check_force_join(user.id, ctx.bot):
        kb = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL.lstrip('@')}")]]
        await update.message.reply_text(
            "⚠️ You must join our channel to watch videos.",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return

    docs = db.collection(COL_VIDEOS).where("code", "==", code).where("active", "==", True).limit(1).get()
    if not docs:
        await update.message.reply_text("❌ Video not found or link has expired.")
        return

    video_doc = docs[0]
    video = video_doc.to_dict()

    try:
        await ctx.bot.send_video(
            chat_id=update.effective_chat.id,
            video=video["file_id"],
            caption=f"🎬 *{video.get('title', 'Video')}*",
            parse_mode=ParseMode.MARKDOWN,
            protect_content=True,
        )
        await record_view(video_doc.id, user.id)
    except TelegramError as e:
        logger.error(f"send_video error: {e}")
        await update.message.reply_text("❌ Failed to send video. Please try again.")

# ─── Admin Menu ───────────────────────────────────────────────────────────────
async def show_admin_menu(update: Update):
    kb = [
        [InlineKeyboardButton("📤 Upload Video", callback_data="adm:upload")],
        [InlineKeyboardButton("🗑 Delete Video", callback_data="adm:delete"),
         InlineKeyboardButton("✏️ Edit Title", callback_data="adm:edit_title")],
        [InlineKeyboardButton("🔗 Generate Link", callback_data="adm:gen_link"),
         InlineKeyboardButton("🔄 Regen Link", callback_data="adm:regen_link")],
        [InlineKeyboardButton("👥 Users", callback_data="adm:users"),
         InlineKeyboardButton("📊 Analytics", callback_data="adm:analytics")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="adm:broadcast")],
        [InlineKeyboardButton("🔧 Maintenance", callback_data="adm:maintenance"),
         InlineKeyboardButton("📋 List Videos", callback_data="adm:list_videos")],
    ]
    text = "🛠 *Admin Panel*\n\nChoose an action:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_admin_menu(update)

# ─── Upload ───────────────────────────────────────────────────────────────────
@admin_only
async def upload_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📤 Send me the video file to upload.")
    return AWAIT_VIDEO

async def upload_receive_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.video:
        await update.message.reply_text("❌ Please send a video file.")
        return AWAIT_VIDEO
    ctx.user_data["upload_file_id"] = update.message.video.file_id
    ctx.user_data["upload_file_size"] = update.message.video.file_size
    ctx.user_data["upload_duration"] = update.message.video.duration
    await update.message.reply_text("✅ Video received!\n\nNow send a *title* for this video:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_TITLE

async def upload_receive_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    title = update.message.text.strip()
    if not title or len(title) > 200:
        await update.message.reply_text("❌ Title must be 1-200 characters.")
        return AWAIT_TITLE

    file_id = ctx.user_data.pop("upload_file_id")
    code = generate_code()
    while db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get():
        code = generate_code()

    bot_info = await ctx.bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={code}"

    db.collection(COL_VIDEOS).add({
        "code": code,
        "file_id": file_id,
        "title": title,
        "link": link,
        "active": True,
        "uploaded_by": update.effective_user.id,
        "uploaded_at": SERVER_TIMESTAMP,
        "views_total": 0,
        "views_daily": {},
        "views_weekly": {},
        "views_monthly": {},
        "file_size": ctx.user_data.pop("upload_file_size", 0),
        "duration": ctx.user_data.pop("upload_duration", 0),
    })

    await update.message.reply_text(
        f"✅ *Video Uploaded!*\n\n"
        f"📌 Title: `{title}`\n"
        f"🔑 Code: `{code}`\n"
        f"🔗 Link:\n`{link}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─── Delete ───────────────────────────────────────────────────────────────────
@admin_only
async def delete_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Send the video *code* to delete:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_DELETE_CODE

async def delete_receive_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
    if not docs:
        await update.message.reply_text("❌ Video not found.")
        return ConversationHandler.END
    docs[0].reference.update({"active": False})
    await update.message.reply_text(f"✅ Video `{code}` has been deactivated.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─── Edit Title ───────────────────────────────────────────────────────────────
@admin_only
async def edit_title_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✏️ Send the video *code* to edit:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_EDIT_CODE

async def edit_title_receive_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
    if not docs:
        await update.message.reply_text("❌ Video not found.")
        return ConversationHandler.END
    ctx.user_data["edit_video_ref"] = docs[0].reference
    await update.message.reply_text("✏️ Send the new title:")
    return AWAIT_EDIT_TITLE

async def edit_title_receive_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    new_title = update.message.text.strip()
    if not new_title or len(new_title) > 200:
        await update.message.reply_text("❌ Title must be 1-200 characters.")
        return AWAIT_EDIT_TITLE
    ctx.user_data.pop("edit_video_ref").update({"title": new_title})
    await update.message.reply_text(f"✅ Title updated to: `{new_title}`", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─── Get Link ─────────────────────────────────────────────────────────────────
@admin_only
async def gen_link_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔗 Send the video *code* to get its link:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_EDIT_CODE

async def gen_link_receive_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
    if not docs:
        await update.message.reply_text("❌ Video not found.")
        return ConversationHandler.END
    video = docs[0].to_dict()
    await update.message.reply_text(
        f"🔗 *Link for* `{video.get('title', code)}`:\n\n`{video['link']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─── Regen Link ───────────────────────────────────────────────────────────────
@admin_only
async def regen_link_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Send the video *code* to regenerate its link:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_EDIT_CODE

async def regen_link_receive_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
    if not docs:
        await update.message.reply_text("❌ Video not found.")
        return ConversationHandler.END
    new_code = generate_code()
    while db.collection(COL_VIDEOS).where("code", "==", new_code).limit(1).get():
        new_code = generate_code()
    bot_info = await ctx.bot.get_me()
    new_link = f"https://t.me/{bot_info.username}?start={new_code}"
    docs[0].reference.update({"code": new_code, "link": new_link})
    await update.message.reply_text(
        f"✅ Link regenerated!\n\n🔑 New Code: `{new_code}`\n🔗 New Link:\n`{new_link}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─── Block / Unblock ──────────────────────────────────────────────────────────
@admin_only
async def block_user_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⛔ Send the user ID to *block*:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_BLOCK_ID

async def block_user_receive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return ConversationHandler.END
    db.collection(COL_USERS).document(str(uid)).set({"blocked": True}, merge=True)
    await update.message.reply_text(f"✅ User `{uid}` has been blocked.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

@admin_only
async def unblock_user_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Send the user ID to *unblock*:", parse_mode=ParseMode.MARKDOWN)
    return AWAIT_UNBLOCK_ID

async def unblock_user_receive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        uid = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return ConversationHandler.END
    db.collection(COL_USERS).document(str(uid)).set({"blocked": False}, merge=True)
    await update.message.reply_text(f"✅ User `{uid}` has been unblocked.", parse_mode=ParseMode.MARKDOWN)
    return ConversationHandler.END

# ─── Analytics ────────────────────────────────────────────────────────────────
@admin_only
async def cmd_analytics(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    today = ts_today()
    week  = ts_week()
    month = ts_month()

    ana_doc = db.collection(COL_ANALYTICS).document("global").get()
    ana = ana_doc.to_dict() if ana_doc.exists else {}

    total_views   = ana.get("views_total", 0)
    daily_views   = ana.get("views_daily", {}).get(today, 0)
    weekly_views  = ana.get("views_weekly", {}).get(week, 0)
    monthly_views = ana.get("views_monthly", {}).get(month, 0)

    all_users    = db.collection(COL_USERS).get()
    total_users  = len(all_users)
    blocked      = sum(1 for u in all_users if u.to_dict().get("blocked"))

    videos       = db.collection(COL_VIDEOS).where("active", "==", True).get()
    total_videos = len(videos)

    top = sorted(videos, key=lambda d: d.to_dict().get("views_total", 0), reverse=True)[:5]
    top_text = ""
    for i, v in enumerate(top, 1):
        vd = v.to_dict()
        top_text += f"\n  {i}. {vd.get('title','?')} — {vd.get('views_total',0)} views"

    msg = update.message or update.callback_query.message
    await msg.reply_text(
        f"📊 *Analytics*\n\n"
        f"👁 Views Today: `{daily_views}`\n"
        f"👁 Views This Week: `{weekly_views}`\n"
        f"👁 Views This Month: `{monthly_views}`\n"
        f"👁 Total Views: `{total_views}`\n\n"
        f"👥 Total Users: `{total_users}`\n"
        f"🚫 Blocked: `{blocked}`\n\n"
        f"🎬 Active Videos: `{total_videos}`\n\n"
        f"🏆 Most Viewed:{top_text}",
        parse_mode=ParseMode.MARKDOWN,
    )

@admin_only
async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    all_users = db.collection(COL_USERS).get()
    total   = len(all_users)
    blocked = sum(1 for u in all_users if u.to_dict().get("blocked"))
    week_ago = now_utc() - timedelta(days=7)
    active = sum(
        1 for u in all_users
        if hasattr(u.to_dict().get("last_seen", ""), "replace") and
        u.to_dict()["last_seen"].replace(tzinfo=timezone.utc) > week_ago
    )
    msg = update.message or update.callback_query.message
    await msg.reply_text(
        f"👥 *User Statistics*\n\n"
        f"Total: `{total}`\n"
        f"Active (7d): `{active}`\n"
        f"Blocked: `{blocked}`",
        parse_mode=ParseMode.MARKDOWN,
    )

@admin_only
async def cmd_list_videos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    videos = db.collection(COL_VIDEOS).where("active", "==", True).order_by(
        "uploaded_at", direction=firestore.Query.DESCENDING).limit(20).get()
    msg = update.message or update.callback_query.message
    if not videos:
        await msg.reply_text("📭 No active videos.")
        return
    lines = ["📋 *Recent Videos (latest 20)*\n"]
    for v in videos:
        vd = v.to_dict()
        lines.append(f"• `{vd['code']}` — {vd.get('title','?')} ({vd.get('views_total',0)} views)")
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

@admin_only
async def cmd_maintenance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    current = await get_setting("maintenance", False)
    new_val = not current
    await set_setting("maintenance", new_val)
    state = "🔧 ON" if new_val else "✅ OFF"
    msg = update.message or update.callback_query.message
    await msg.reply_text(f"Maintenance mode is now *{state}*.", parse_mode=ParseMode.MARKDOWN)

# ─── Broadcast ───────────────────────────────────────────────────────────────
@admin_only
async def broadcast_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📢 *Broadcast*\n\nSend me the content to broadcast.\nSupports: text, photo, video.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return AWAIT_BROADCAST_CONTENT

async def broadcast_receive_content(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["bc_message_id"] = update.message.message_id
    ctx.user_data["bc_chat_id"] = update.message.chat_id
    user_count = len(db.collection(COL_USERS).where("blocked", "==", False).get())
    kb = [[
        InlineKeyboardButton("✅ Send Now", callback_data="bc:confirm"),
        InlineKeyboardButton("❌ Cancel", callback_data="bc:cancel"),
    ]]
    await update.message.reply_text(
        f"📢 Ready to broadcast to *{user_count}* users.\n\nConfirm?",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode=ParseMode.MARKDOWN,
    )
    return AWAIT_BROADCAST_CONFIRM

async def broadcast_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "bc:cancel":
        ctx.user_data.pop("bc_message_id", None)
        await q.edit_message_text("❌ Broadcast cancelled.")
        return ConversationHandler.END

    src_chat = ctx.user_data.pop("bc_chat_id")
    src_msg  = ctx.user_data.pop("bc_message_id")
    users = db.collection(COL_USERS).where("blocked", "==", False).get()
    total = len(users)
    sent = failed = blocked_count = deactivated = 0

    progress_msg = await q.edit_message_text(f"📢 Broadcasting to {total} users...\n\n⏳ Starting...")
    start_time = time.time()

    for i, u_doc in enumerate(users):
        uid = int(u_doc.id)
        try:
            await ctx.bot.copy_message(chat_id=uid, from_chat_id=src_chat, message_id=src_msg)
            sent += 1
        except Forbidden:
            blocked_count += 1
            db.collection(COL_USERS).document(str(uid)).update({"blocked": True})
        except BadRequest as e:
            if "chat not found" in str(e).lower():
                deactivated += 1
            else:
                failed += 1
        except TelegramError:
            failed += 1

        if (i + 1) % 50 == 0:
            try:
                await progress_msg.edit_text(
                    f"📢 Broadcasting... {i+1}/{total}\n✅ Sent: {sent} | ❌ Failed: {failed}"
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)

    elapsed = round(time.time() - start_time)
    db.collection(COL_BROADCAST).add({
        "sent": sent, "failed": failed, "blocked": blocked_count,
        "deactivated": deactivated, "total": total,
        "by": update.effective_user.id, "ts": SERVER_TIMESTAMP, "elapsed_sec": elapsed,
    })
    await progress_msg.edit_text(
        f"📢 *Broadcast Complete!*\n\n"
        f"👥 Total: `{total}`\n✅ Sent: `{sent}`\n"
        f"🚫 Blocked: `{blocked_count}`\n💤 Deactivated: `{deactivated}`\n"
        f"❌ Failed: `{failed}`\n⏱ Time: `{elapsed}s`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

# ─── Callback Router ─────────────────────────────────────────────────────────
async def callback_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await q.answer("⛔ Admins only.", show_alert=True)
        return
    data = q.data
    if data == "adm:analytics":
        await cmd_analytics(update, ctx)
    elif data == "adm:users":
        await cmd_users(update, ctx)
    elif data == "adm:list_videos":
        await cmd_list_videos(update, ctx)
    elif data == "adm:maintenance":
        await cmd_maintenance(update, ctx)
    elif data == "adm:back":
        await show_admin_menu(update)
    else:
        await q.edit_message_text("Please use the menu commands. Type /menu")

# ─── Cancel ───────────────────────────────────────────────────────────────────
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

# ─── Build App ────────────────────────────────────────────────────────────────
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()
    cancel_handler = CommandHandler("cancel", cmd_cancel)

    upload_conv = ConversationHandler(
        entry_points=[CommandHandler("upload", upload_start)],
        states={
            AWAIT_VIDEO: [MessageHandler(filters.VIDEO, upload_receive_video)],
            AWAIT_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, upload_receive_title)],
        },
        fallbacks=[cancel_handler],
    )
    delete_conv = ConversationHandler(
        entry_points=[CommandHandler("delete", delete_start)],
        states={
            AWAIT_DELETE_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delete_receive_code)],
        },
        fallbacks=[cancel_handler],
    )
    edit_title_conv = ConversationHandler(
        entry_points=[CommandHandler("edittitle", edit_title_start)],
        states={
            AWAIT_EDIT_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_title_receive_code)],
            AWAIT_EDIT_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_title_receive_title)],
        },
        fallbacks=[cancel_handler],
    )
    gen_link_conv = ConversationHandler(
        entry_points=[CommandHandler("getlink", gen_link_start)],
        states={
            AWAIT_EDIT_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, gen_link_receive_code)],
        },
        fallbacks=[cancel_handler],
    )
    regen_link_conv = ConversationHandler(
        entry_points=[CommandHandler("regenlink", regen_link_start)],
        states={
            AWAIT_EDIT_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, regen_link_receive_code)],
        },
        fallbacks=[cancel_handler],
    )
    block_conv = ConversationHandler(
        entry_points=[CommandHandler("block", block_user_start)],
        states={
            AWAIT_BLOCK_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, block_user_receive)],
        },
        fallbacks=[cancel_handler],
    )
    unblock_conv = ConversationHandler(
        entry_points=[CommandHandler("unblock", unblock_user_start)],
        states={
            AWAIT_UNBLOCK_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, unblock_user_receive)],
        },
        fallbacks=[cancel_handler],
    )
    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("broadcast", broadcast_start)],
        states={
            AWAIT_BROADCAST_CONTENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_receive_content),
                MessageHandler(filters.PHOTO, broadcast_receive_content),
                MessageHandler(filters.VIDEO, broadcast_receive_content),
            ],
            AWAIT_BROADCAST_CONFIRM: [
                CallbackQueryHandler(broadcast_callback, pattern="^bc:"),
            ],
        },
        fallbacks=[cancel_handler],
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("analytics", cmd_analytics))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("videos", cmd_list_videos))
    app.add_handler(CommandHandler("maintenance", cmd_maintenance))
    app.add_handler(upload_conv)
    app.add_handler(delete_conv)
    app.add_handler(edit_title_conv)
    app.add_handler(gen_link_conv)
    app.add_handler(regen_link_conv)
    app.add_handler(block_conv)
    app.add_handler(unblock_conv)
    app.add_handler(broadcast_conv)
    app.add_handler(CallbackQueryHandler(callback_router))

    return app

# ─── Main ────────────────────────────────────────────────────────────────────
import asyncio

async def main():
    application = build_app()
    logger.info("Bot started.")
    await application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
