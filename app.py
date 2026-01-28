# app.py (Final Streaming-Ready Version)
import os
import asyncio
import secrets
import traceback
import math
from contextlib import asynccontextmanager

from pyrogram import Client, filters, enums, raw
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from config import Config
from database import db

# ==============================================
# SETUP
# ==============================================
bot = Client("StreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}
work_loads = {}
class_cache = {}

templates = Jinja2Templates(directory="templates")
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await bot.start()
    me = await bot.get_me()
    Config.BOT_USERNAME = me.username
    multi_clients[0] = bot
    work_loads[0] = 0
    print(f"✅ Bot @{Config.BOT_USERNAME} started")
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)

# ==============================================
# HELPERS
# ==============================================
def get_readable_size(size):
    for unit in ['B','KB','MB','GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024

def sanitize_filename(name):
    return "".join(c for c in name if c.isalnum() or c in ('.','_','-')).strip()

# ==============================================
# BOT HANDLERS (Updated for Custom Naming)
# ==============================================
waiting_for_name = {}

@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, message: Message):
    await message.reply_text(f"👋 Hello {message.from_user.first_name}! Send me any video/file to get link.")

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, message: Message):
    media = message.document or message.video or message.audio
    
    # --- Check duplicate (আগে থেকেই আছে কিনা) ---
    existing = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if existing:
        u_id = existing["_id"]
        msg_id = existing["message_id"]
        f_name = existing.get("file_name", "video.mkv")
        
        direct_link = f"{Config.BASE_URL}/dl/{msg_id}/{sanitize_filename(f_name)}"
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]])
        return await message.reply_text(f"✅ **File already exists!**\n\n🔗 Direct Link: `{direct_link}`", reply_markup=btn, quote=True)
    
    # নাম চাওয়ার জন্য ওয়েটিং লিস্টে রাখা
    waiting_for_name[message.from_user.id] = message
    await message.reply_text("📝 **Please send a Name for this file:**")

@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name(client, message):
    uid = message.from_user.id
    if uid not in waiting_for_name:
        return

    # আগের পাঠানো ফাইলটি উদ্ধার করা
    orig_msg = waiting_for_name.pop(uid)
    media = orig_msg.document or orig_msg.video or orig_msg.audio
    
    # এক্সটেনশন বের করা
    user_input_name = message.text.replace(" ", "_") # স্পেস থাকলে আন্ডারস্কোর করে দেওয়া
    ext = os.path.splitext(media.file_name or "video.mkv")[1] or ".mkv"
    
    # আপনার চাহিদা অনুযায়ী নাম ফরম্যাট করা
    final_file_name = f"[Moviedekhobd.rf.gd] {user_input_name}[Moviedekhobd.rf.gd]{ext}"
    
    sts = await message.reply_text("🚀 **Processing and Storing...**")
    
    try:
        # স্টোরেজ চ্যানেলে কপি পাঠানো
        sent = await orig_msg.copy(chat_id=int(Config.STORAGE_CHANNEL))
        u_id = secrets.token_urlsafe(8)
        msg_id = sent.id
        
        # ডাটাবেসে সেভ করা
        await db.collection.insert_one({
            "_id": u_id, 
            "message_id": msg_id, 
            "file_unique_id": media.file_unique_id, 
            "file_name": final_file_name
        })

        direct_link = f"{Config.BASE_URL}/dl/{msg_id}/{sanitize_filename(final_file_name)}"
        
        btn = InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]])
        
        await sts.delete()
        await message.reply_text(
            f"✅ **Success! File Stored.**\n\n📄 Name: `{final_file_name}`\n🔗 Direct Link: `{direct_link}`", 
            reply_markup=btn, 
            quote=True
        )
        
    except Exception:
        await message.reply_text("❌ Failed to process name and store file.")
        print(traceback.format_exc())

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, message: Message):
    await handle_file_upload(message)

# ==============================================
# STREAMING ENGINE
# ==============================================
class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client

    async def yield_file(self, f, i, offset, fc, lc, pc, cs):
        work_loads[i] += 1
        try:
            ms = self.client.media_sessions.get(f.dc_id)
            if not ms:
                if f.dc_id != await self.client.storage.dc_id():
                    auth = await Auth(self.client, f.dc_id, await self.client.storage.test_mode()).create()
                    ms = Session(self.client, f.dc_id, auth, await self.client.storage.test_mode(), is_media=True)
                    await ms.start()
                    exp = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=f.dc_id))
                    await ms.invoke(raw.functions.auth.ImportAuthorization(id=exp.id, bytes=exp.bytes))
                else: ms = self.client.session
                self.client.media_sessions[f.dc_id] = ms

            loc = raw.types.InputDocumentFileLocation(id=f.media_id, access_hash=f.access_hash,
                                                     file_reference=f.file_reference, thumb_size=f.thumbnail_size)

            for chunk in range(1, pc + 1):
                r = await ms.invoke(raw.functions.upload.GetFile(location=loc, offset=offset, limit=cs), retries=0)
                if not r.bytes: break
                if pc == 1: yield r.bytes[fc:lc]
                elif chunk == 1: yield r.bytes[fc:]
                elif chunk == pc: yield r.bytes[:lc]
                else: yield r.bytes
                offset += cs
        finally: work_loads[i] -= 1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r: Request, mid: int, fname: str):
    idx = min(work_loads, key=work_loads.get)
    c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c)
    class_cache[c] = st

    try:
        msg = await c.get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video or msg.audio
        fid = FileId.decode(m.file_id)
        size = m.file_size

        # Range headers
        rh = r.headers.get("Range", "")
        fb, ub = 0, size - 1
        if rh:
            parts = rh.replace("bytes=", "").split("-")
            fb = int(parts[0])
            if len(parts) > 1 and parts[1]: ub = int(parts[1])
        rl = ub - fb + 1
        cs = 1024 * 512  # 512 KB chunk
        off = (fb // cs) * cs
        fc = fb - off
        lc = (ub % cs) + 1
        pc = math.ceil(rl / cs)

        headers = {
            "Content-Type": m.mime_type or "video/x-matroska",
            "Accept-Ranges": "bytes",
            "Content-Length": str(rl),
            "Content-Disposition": f'inline; filename="{m.file_name}"'
        }
        if rh: headers["Content-Range"] = f"bytes {fb}-{ub}/{size}"

        return StreamingResponse(st.yield_file(fid, idx, off, fc, lc, pc, cs),
                                 status_code=206 if rh else 200,
                                 headers=headers)
    except Exception:
        raise HTTPException(404)

# ==============================================
# SHOW PAGE
# ==============================================
@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request})

# ==============================================
# HEALTH CHECK
# ==============================================
@app.get("/")
async def health(): return {"status": "ok"}

# ==============================================
# RUN
# ==============================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
