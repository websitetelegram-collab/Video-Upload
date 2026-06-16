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
    AWAIT_CHANNEL_LINK,
) = range(10)

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

# ─── Admin Panel ──────────────────────────────────────────────────────────────
def build_admin_keyboard() -> InlineKeyboardMarkup:
    """Build the full admin panel as inline keyboard buttons."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📤 Upload Video",   callback_data="adm:upload"),
            InlineKeyboardButton("🗑 Delete Video",   callback_data="adm:delete"),
        ],
        [
            InlineKeyboardButton("✏️ Edit Title",     callback_data="adm:edit_title"),
            InlineKeyboardButton("🔗 Get Link",       callback_data="adm:gen_link"),
        ],
        [
            InlineKeyboardButton("🔄 Regen Link",     callback_data="adm:regen_link"),
            InlineKeyboardButton("📋 List Videos",    callback_data="adm:list_videos"),
        ],
        [
            InlineKeyboardButton("👥 User Stats",     callback_data="adm:users"),
            InlineKeyboardButton("📊 Analytics",      callback_data="adm:analytics"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast",      callback_data="adm:broadcast"),
            InlineKeyboardButton("🔧 Maintenance",    callback_data="adm:maintenance"),
        ],
        [
            InlineKeyboardButton("🚫 Block User",     callback_data="adm:block"),
            InlineKeyboardButton("✅ Unblock User",   callback_data="adm:unblock"),
        ],
        [
            InlineKeyboardButton("📡 Set Channel",    callback_data="adm:set_channel"),
        ],
    ])

async def show_admin_panel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send or edit the admin panel message with inline keyboard."""
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        return

    # Get current maintenance & channel info for status display
    maint = await get_setting("maintenance", False)
    ch = await get_setting("force_join_channel", FORCE_JOIN_CHANNEL or "Not set")
    maint_icon = "🔧 ON" if maint else "✅ OFF"

    text = (
        "🛠 *Admin Panel*\n\n"
        f"📡 Force Join: `{ch}`\n"
        f"🔧 Maintenance: `{maint_icon}`\n\n"
        "Select an action:"
    )
    kb = build_admin_keyboard()

    q = update.callback_query
    if q:
        try:
            await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            await q.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)

# ─── /start ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if rate_limited(user.id):
        await update.message.reply_text("🐢 Slow down — try again shortly.")
        return

    await upsert_user(user)

    # If admin, always show admin panel (regardless of args)
    if user.id in ADMIN_IDS:
        args = ctx.args
        if args:
            await handle_video_request(update, ctx, args[0])
        else:
            await show_admin_panel(update, ctx)
        return

    # --- Regular user flow ---
    args = ctx.args
    if not args:
        if await is_maintenance():
            await update.message.reply_text("🔧 Bot is under maintenance. Please check back later.")
            return
        await update.message.reply_text("👋 Welcome!\n\nUse your video link to watch content.")
        return

    await handle_video_request(update, ctx, args[0])

async def handle_video_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, code: str):
    user = update.effective_user

    if await is_maintenance() and user.id not in ADMIN_IDS:
        await update.message.reply_text("🔧 Bot is under maintenance.")
        return

    u_doc = db.collection(COL_USERS).document(str(user.id)).get()
    if u_doc.exists and u_doc.to_dict().get("blocked"):
        await update.message.reply_text("⛔ You have been blocked from using this bot.")
        return

    # Check force join — read from Firestore setting first, fall back to env
    active_channel = await get_setting("force_join_channel", FORCE_JOIN_CHANNEL or "")
    if active_channel:
        try:
            member = await ctx.bot.get_chat_member(active_channel, user.id)
            joined = member.status not in ("left", "kicked")
        except Exception:
            joined = True
        if not joined:
            ch_clean = active_channel.lstrip("@")
            kb = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{ch_clean}")]]
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

# ─── Unknown Command — block non-admins ──────────────────────────────────────
async def cmd_unknown(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in ADMIN_IDS:
        await show_admin_panel(update, ctx)
        return
    await update.message.reply_text("❌ Invalid Command.")

# ─── Callback Router ─────────────────────────────────────────────────────────
async def callback_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = update.effective_user.id

    # Security — only admins
    if uid not in ADMIN_IDS:
        await q.answer("⛔ Access denied.", show_alert=True)
        return

    await q.answer()
    data = q.data

    # ── Back to panel ──────────────────────────────────────────────────────────
    if data in ("adm:back", "adm:panel"):
        ctx.user_data.clear()
        await show_admin_panel(update, ctx)
        return

    # ── Inline info buttons — send NEW message below, keep panel intact ────────
    if data == "adm:analytics":
        await _send_analytics(q.message)
        return

    if data == "adm:users":
        await _send_users(q.message)
        return

    if data == "adm:list_videos":
        await _send_list_videos(q.message)
        return

    if data == "adm:maintenance":
        current = await get_setting("maintenance", False)
        new_val = not current
        await set_setting("maintenance", new_val)
        icon = "🔧 ON" if new_val else "✅ OFF"
        await q.message.reply_text(f"🔧 Maintenance mode: *{icon}*", parse_mode=ParseMode.MARKDOWN)
        # Refresh panel to show new status
        await show_admin_panel(update, ctx)
        return

    # ── State-driven actions — edit panel message to show prompt ───────────────
    if data == "adm:upload":
        await q.edit_message_text(
            "📤 *Upload Video*\n\nSend me the video file now.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_VIDEO
        return

    if data == "adm:delete":
        await q.edit_message_text(
            "🗑 *Delete Video*\n\nSend the video *code* to delete.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_DELETE_CODE
        return

    if data == "adm:edit_title":
        await q.edit_message_text(
            "✏️ *Edit Title*\n\nSend the video *code* to edit its title.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_EDIT_CODE
        return

    if data == "adm:gen_link":
        await q.edit_message_text(
            "🔗 *Get Link*\n\nSend the video *code* to retrieve its link.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = "gen_link"
        return

    if data == "adm:regen_link":
        await q.edit_message_text(
            "🔄 *Regenerate Link*\n\nSend the video *code* to generate a new link.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = "regen_link"
        return

    if data == "adm:block":
        await q.edit_message_text(
            "🚫 *Block User*\n\nSend the user ID to block.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_BLOCK_ID
        return

    if data == "adm:unblock":
        await q.edit_message_text(
            "✅ *Unblock User*\n\nSend the user ID to unblock.\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_UNBLOCK_ID
        return

    if data == "adm:broadcast":
        await q.edit_message_text(
            "📢 *Broadcast*\n\nSend the content to broadcast (text, photo, or video).\n\n/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_BROADCAST_CONTENT
        return

    if data == "adm:set_channel":
        current_ch = await get_setting("force_join_channel", FORCE_JOIN_CHANNEL or "Not set")
        await q.edit_message_text(
            f"📡 *Force Join Channel*\n\n"
            f"Current: `{current_ch}`\n\n"
            "Send the channel username (e.g. `@mychannel`) to update,\n"
            "or send `off` to disable force join.\n\n"
            "/cancel to go back.",
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_CHANNEL_LINK
        return

    # ── Broadcast confirm/cancel (from universal handler broadcast flow) ────────
    if data == "bc:cancel":
        ctx.user_data.pop("bc_message_id", None)
        ctx.user_data.pop("bc_chat_id", None)
        ctx.user_data.pop("state", None)
        await q.edit_message_text("❌ Broadcast cancelled.")
        return

    if data == "bc:confirm":
        src_chat = ctx.user_data.pop("bc_chat_id", None)
        src_msg  = ctx.user_data.pop("bc_message_id", None)
        ctx.user_data.pop("state", None)
        if not src_chat or not src_msg:
            await q.edit_message_text("❌ Broadcast data lost. Please try again.")
            return
        await _do_broadcast(q, ctx, src_chat, src_msg, update.effective_user.id)
        return

# ─── Broadcast execution helper ───────────────────────────────────────────────
async def _do_broadcast(q, ctx, src_chat: int, src_msg: int, admin_id: int):
    users = db.collection(COL_USERS).where("blocked", "==", False).get()
    total = len(users)
    sent = failed = blocked_count = deactivated = 0

    progress_msg = await q.edit_message_text(f"📢 Broadcasting to {total} users...\n\n⏳ Starting...")
    start_time = time.time()

    for i, u_doc in enumerate(users):
        t_uid = int(u_doc.id)
        try:
            await ctx.bot.copy_message(chat_id=t_uid, from_chat_id=src_chat, message_id=src_msg)
            sent += 1
        except Forbidden:
            blocked_count += 1
            db.collection(COL_USERS).document(str(t_uid)).update({"blocked": True})
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
        "by": admin_id, "ts": SERVER_TIMESTAMP, "elapsed_sec": elapsed,
    })
    await progress_msg.edit_text(
        f"📢 *Broadcast Complete!*\n\n"
        f"👥 Total: `{total}`\n✅ Sent: `{sent}`\n"
        f"🚫 Blocked: `{blocked_count}`\n💤 Deactivated: `{deactivated}`\n"
        f"❌ Failed: `{failed}`\n⏱ Time: `{elapsed}s`",
        parse_mode=ParseMode.MARKDOWN,
    )

# ─── Analytics / Users / List Videos helpers (send as NEW message) ───────────
async def _send_analytics(msg):
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

    await msg.reply_text(
        f"📊 *Analytics*\n\n"
        f"👁 Today: `{daily_views}`\n"
        f"👁 This Week: `{weekly_views}`\n"
        f"👁 This Month: `{monthly_views}`\n"
        f"👁 Total Views: `{total_views}`\n\n"
        f"👥 Total Users: `{total_users}`\n"
        f"🚫 Blocked: `{blocked}`\n\n"
        f"🎬 Active Videos: `{total_videos}`\n\n"
        f"🏆 Most Viewed:{top_text}",
        parse_mode=ParseMode.MARKDOWN,
    )

async def _send_users(msg):
    all_users = db.collection(COL_USERS).get()
    total   = len(all_users)
    blocked = sum(1 for u in all_users if u.to_dict().get("blocked"))
    week_ago = now_utc() - timedelta(days=7)
    active = sum(
        1 for u in all_users
        if hasattr(u.to_dict().get("last_seen", ""), "replace") and
        u.to_dict()["last_seen"].replace(tzinfo=timezone.utc) > week_ago
    )
    await msg.reply_text(
        f"👥 *User Statistics*\n\n"
        f"Total: `{total}`\n"
        f"Active (7d): `{active}`\n"
        f"Blocked: `{blocked}`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def _send_list_videos(msg):
    videos = db.collection(COL_VIDEOS).where("active", "==", True).order_by(
        "uploaded_at", direction=firestore.Query.DESCENDING).limit(20).get()
    if not videos:
        await msg.reply_text("📭 No active videos.")
        return
    lines = ["📋 *Recent Videos (latest 20)*\n"]
    for v in videos:
        vd = v.to_dict()
        lines.append(f"• `{vd['code']}` — {vd.get('title','?')} ({vd.get('views_total',0)} views)")
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

# ─── Universal Message Handler — handles all admin states ─────────────────────
async def universal_message_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # Non-admins: silently ignore all plain messages
    if uid not in ADMIN_IDS:
        return

    state = ctx.user_data.get("state")
    msg   = update.message

    # ── AWAIT_VIDEO: waiting for a video file ─────────────────────────────────
    if state == AWAIT_VIDEO:
        if msg.video:
            ctx.user_data["upload_file_id"]   = msg.video.file_id
            ctx.user_data["upload_file_size"]  = msg.video.file_size
            ctx.user_data["upload_duration"]   = msg.video.duration
            ctx.user_data["state"] = AWAIT_TITLE
            await msg.reply_text(
                "✅ Video received!\n\nNow send a *title* for this video:",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.reply_text("❌ Please send a video file.")
        return

    # ── Direct video drop (no active state) ──────────────────────────────────
    if msg.video and not state:
        ctx.user_data["upload_file_id"]   = msg.video.file_id
        ctx.user_data["upload_file_size"]  = msg.video.file_size
        ctx.user_data["upload_duration"]   = msg.video.duration
        ctx.user_data["state"] = AWAIT_TITLE
        await msg.reply_text(
            "✅ Video received!\n\nNow send a *title* for this video:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # ── AWAIT_TITLE ───────────────────────────────────────────────────────────
    if state == AWAIT_TITLE:
        if not msg.text:
            await msg.reply_text("❌ Please send a text title.")
            return
        title = msg.text.strip()
        if not title or len(title) > 200:
            await msg.reply_text("❌ Title must be 1–200 characters.")
            return
        file_id = ctx.user_data.pop("upload_file_id", None)
        if not file_id:
            await msg.reply_text("❌ Video file lost. Please start again.")
            ctx.user_data.clear()
            return
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
            "uploaded_by": uid,
            "uploaded_at": SERVER_TIMESTAMP,
            "views_total": 0,
            "views_daily": {},
            "views_weekly": {},
            "views_monthly": {},
            "file_size": ctx.user_data.pop("upload_file_size", 0),
            "duration": ctx.user_data.pop("upload_duration", 0),
        })
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        await msg.reply_text(
            f"✅ *Video Uploaded!*\n\n"
            f"📌 Title: `{title}`\n"
            f"🔑 Code: `{code}`\n"
            f"🔗 Link:\n`{link}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=back_kb,
        )
        return

    # ── AWAIT_DELETE_CODE ─────────────────────────────────────────────────────
    if state == AWAIT_DELETE_CODE:
        code = msg.text.strip() if msg.text else ""
        docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        if not docs:
            await msg.reply_text("❌ Video not found.", reply_markup=back_kb)
        else:
            db.collection(COL_VIDEOS).document(docs[0].id).update({"active": False})
            await msg.reply_text(
                f"✅ Video `{code}` deleted.", parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
            )
        return

    # ── AWAIT_EDIT_CODE ───────────────────────────────────────────────────────
    if state == AWAIT_EDIT_CODE:
        code = msg.text.strip() if msg.text else ""
        docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
        if not docs:
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
            await msg.reply_text("❌ Video not found.", reply_markup=back_kb)
            ctx.user_data.pop("state", None)
            return
        ctx.user_data["edit_code"] = code
        ctx.user_data["state"] = AWAIT_EDIT_TITLE
        await msg.reply_text("✏️ Now send the new *title*:", parse_mode=ParseMode.MARKDOWN)
        return

    # ── AWAIT_EDIT_TITLE ──────────────────────────────────────────────────────
    if state == AWAIT_EDIT_TITLE:
        title = msg.text.strip() if msg.text else ""
        if not title or len(title) > 200:
            await msg.reply_text("❌ Title must be 1–200 characters.")
            return
        code = ctx.user_data.pop("edit_code", "")
        docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        if not docs:
            await msg.reply_text("❌ Video not found.", reply_markup=back_kb)
        else:
            db.collection(COL_VIDEOS).document(docs[0].id).update({"title": title})
            await msg.reply_text(
                f"✅ Title updated to `{title}`", parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
            )
        return

    # ── gen_link ──────────────────────────────────────────────────────────────
    if state == "gen_link":
        code = msg.text.strip() if msg.text else ""
        docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        if not docs:
            await msg.reply_text("❌ Video not found.", reply_markup=back_kb)
        else:
            video = docs[0].to_dict()
            await msg.reply_text(
                f"🔗 Link:\n`{video.get('link', '')}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=back_kb,
            )
        return

    # ── regen_link ────────────────────────────────────────────────────────────
    if state == "regen_link":
        code = msg.text.strip() if msg.text else ""
        docs = db.collection(COL_VIDEOS).where("code", "==", code).limit(1).get()
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        if not docs:
            await msg.reply_text("❌ Video not found.", reply_markup=back_kb)
        else:
            new_code = generate_code()
            while db.collection(COL_VIDEOS).where("code", "==", new_code).limit(1).get():
                new_code = generate_code()
            bot_info = await ctx.bot.get_me()
            new_link = f"https://t.me/{bot_info.username}?start={new_code}"
            db.collection(COL_VIDEOS).document(docs[0].id).update({"code": new_code, "link": new_link})
            await msg.reply_text(
                f"🔄 New link:\n`{new_link}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=back_kb,
            )
        return

    # ── AWAIT_BLOCK_ID ────────────────────────────────────────────────────────
    if state == AWAIT_BLOCK_ID:
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        try:
            target = int(msg.text.strip() if msg.text else "")
        except ValueError:
            await msg.reply_text("❌ Invalid user ID.", reply_markup=back_kb)
            return
        db.collection(COL_USERS).document(str(target)).set({"blocked": True}, merge=True)
        await msg.reply_text(
            f"✅ User `{target}` blocked.", parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
        )
        return

    # ── AWAIT_UNBLOCK_ID ──────────────────────────────────────────────────────
    if state == AWAIT_UNBLOCK_ID:
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        try:
            target = int(msg.text.strip() if msg.text else "")
        except ValueError:
            await msg.reply_text("❌ Invalid user ID.", reply_markup=back_kb)
            return
        db.collection(COL_USERS).document(str(target)).set({"blocked": False}, merge=True)
        await msg.reply_text(
            f"✅ User `{target}` unblocked.", parse_mode=ParseMode.MARKDOWN, reply_markup=back_kb
        )
        return

    # ── AWAIT_CHANNEL_LINK ────────────────────────────────────────────────────
    if state == AWAIT_CHANNEL_LINK:
        ctx.user_data.pop("state", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")]])
        raw = msg.text.strip() if msg.text else ""
        if raw.lower() == "off":
            await set_setting("force_join_channel", "")
            await msg.reply_text("✅ Force join disabled.", reply_markup=back_kb)
        else:
            ch = raw if raw.startswith("@") else f"@{raw}"
            await set_setting("force_join_channel", ch)
            await msg.reply_text(
                f"✅ Force join channel set to `{ch}`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=back_kb,
            )
        return

    # ── AWAIT_BROADCAST_CONTENT ───────────────────────────────────────────────
    if state == AWAIT_BROADCAST_CONTENT:
        ctx.user_data["bc_message_id"] = msg.message_id
        ctx.user_data["bc_chat_id"]    = msg.chat_id
        user_count = len(db.collection(COL_USERS).where("blocked", "==", False).get())
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Send Now", callback_data="bc:confirm"),
            InlineKeyboardButton("❌ Cancel",   callback_data="bc:cancel"),
        ]])
        await msg.reply_text(
            f"📢 Ready to broadcast to *{user_count}* users.\n\nConfirm?",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )
        ctx.user_data["state"] = AWAIT_BROADCAST_CONFIRM
        return

    # ── AWAIT_BROADCAST_CONFIRM — waiting for button ──────────────────────────
    if state == AWAIT_BROADCAST_CONFIRM:
        await msg.reply_text("⏳ Please tap ✅ Send Now or ❌ Cancel above.")
        return

# ─── /cancel ─────────────────────────────────────────────────────────────────
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("❌ Invalid Command.")
        return
    ctx.user_data.clear()
    await update.message.reply_text(
        "❌ Operation cancelled.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Back to Panel", callback_data="adm:back")
        ]]),
    )

# ─── Build App ────────────────────────────────────────────────────────────────
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Only /start and /cancel are registered as named commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    # All callback buttons route through callback_router
    app.add_handler(CallbackQueryHandler(callback_router))

    # All plain messages (text + video) from admins go to universal_message_handler
    app.add_handler(MessageHandler(
        filters.VIDEO | (filters.TEXT & ~filters.COMMAND),
        universal_message_handler,
    ))

    # Catch-all: any other command → unknown handler (last)
    app.add_handler(MessageHandler(filters.COMMAND, cmd_unknown))

    return app

# ─── Main ────────────────────────────────────────────────────────────────────
import asyncio
import os
from aiohttp import web

async def health_check(request):
    return web.Response(text="OK")

async def run_http_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    port = int(os.environ.get("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health check server running on port {port}")

async def manual_polling(application):
    """Manually fetch and process updates to avoid PTB event loop conflicts."""
    bot = application.bot
    offset = None

    updates = await bot.get_updates(offset=-1, timeout=1)
    if updates:
        offset = updates[-1].update_id + 1

    logger.info("Polling started, waiting for updates...")

    while True:
        try:
            updates = await bot.get_updates(
                offset=offset,
                timeout=30,
                allowed_updates=["message", "callback_query", "chat_member"],
            )
            for update in updates:
                offset = update.update_id + 1
                await application.process_update(update)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            await asyncio.sleep(3)

async def main():
    application = build_app()
    logger.info("Bot started.")
    await run_http_server()
    await application.initialize()
    await application.start()
    try:
        await manual_polling(application)
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
