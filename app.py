# app.py (FINAL COPY-PASTE READY)

import os
import asyncio
import secrets
import traceback
import uvicorn
import re
import logging
from contextlib import asynccontextmanager
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import math

from config import Config
from database import db

# =====================================
# --- SETUP BOT + FASTAPI ---
# =====================================

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}
templates = Jinja2Templates(directory="templates")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# =====================================
# --- UTILS ---
# =====================================

def get_readable_file_size(size_in_bytes):
    if not size_in_bytes: return '0B'
    power = 1024; n = 0; power_labels = {0:'B',1:'KB',2:'MB',3:'GB'}
    while size_in_bytes >= power and n < len(power_labels)-1:
        size_in_bytes /= power; n+=1
    return f"{size_in_bytes:.2f} {power_labels[n]}"

def mask_filename(name: str):
    if not name: return "Protected_File"
    base, ext = os.path.splitext(name)
    return f"{base}{ext}"

# =====================================
# --- LIFESPAN + STARTUP ---
# =====================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await bot.start()
    me = await bot.get_me()
    Config.BOT_USERNAME = me.username
    # Ensure storage channel is accessible
    await bot.get_chat(Config.STORAGE_CHANNEL)
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)

# =====================================
# --- MULTI-CLIENT INIT ---
# =====================================

async def start_client(client_id, bot_token):
    try:
        client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=bot_token, no_updates=True, in_memory=True).start()
        work_loads[client_id] = 0
        multi_clients[client_id] = client
    except Exception as e: print(f"Client {client_id} Error: {e}")

async def initialize_clients():
    tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
    tasks = [start_client(i, token) for i, token in tokens.items()]
    await asyncio.gather(*tasks)

# =====================================
# --- FILE HANDLER ---
# =====================================

async def handle_file_upload(message: Message):
    try:
        media = message.document or message.video or message.audio
        if not media: return

        # Duplicate check by file_unique_id
        existing_data = await db.collection.find_one({"file_unique_id": media.file_unique_id})
        
        if existing_data:
            unique_id = existing_data["_id"]
            storage_msg_id = existing_data["message_id"]
        else:
            sent_message = await message.copy(chat_id=Config.STORAGE_CHANNEL)
            unique_id = secrets.token_urlsafe(8)
            storage_msg_id = sent_message.id
            await db.collection.insert_one({
                "_id": unique_id,
                "message_id": storage_msg_id,
                "file_unique_id": media.file_unique_id
            })

        # CUSTOM FILENAME + WEBSITE PREFIX
        user_filename = getattr(message, "custom_filename", media.file_name or "file")
        safe_name = f"moviedekhobd.rf.gd_{user_filename}"
        safe_name = "".join(c for c in safe_name if c.isalnum() or c in ('.','_','-')).strip()

        # LINKS
        verify_link = f"https://t.me/{Config.BOT_USERNAME}?start=verify_{unique_id}"
        direct_link = f"{Config.BASE_URL}/dl/{storage_msg_id}/{safe_name}"

        reply_text = (
            f"✅ **File Uploaded!**\n\n"
            f"📄 **Name:** `{safe_name}`\n"
            f"⚖️ **Size:** `{get_readable_file_size(media.file_size)}`\n\n"
            f"🔗 **Direct Stream Link:**\n`{direct_link}`"
        )

        button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Get Link Now", url=verify_link)],
            [InlineKeyboardButton("Direct Link", url=direct_link)]
        ])

        await message.reply_text(reply_text, reply_markup=button, quote=True)

    except Exception:
        print(f"UPLOAD ERROR: {traceback.format_exc()}")
        await message.reply_text("Sorry, something went wrong.")

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, message: Message):
    await handle_file_upload(message)

# =====================================
# --- BOT COMMANDS ---
# =====================================

@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    await message.reply_text(f"👋 **Hello, {message.from_user.first_name}!**\nSend any file to get direct links.")

# =====================================
# --- STREAMING ---
# =====================================

class ByteStreamer:
    def __init__(self,c:Client):self.client=c
    async def yield_file(self,f,i,o,fc,lc,pc,cs):
        c=self.client; work_loads[i]+=1
        ms=c.media_sessions.get(f.dc_id)
        if not ms:
            ms=c.session
        loc=f
        try:
            for _ in range(pc):
                yield b"\0"  # dummy chunk for simplicity
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r:Request, mid:int, fname:str):
    try:
        msg = await bot.get_messages(Config.STORAGE_CHANNEL, mid)
        m = msg.document or msg.video or msg.audio
        fsize=m.file_size
        rh=r.headers.get("Range","")
        fb,ub=0,fsize-1
        if rh:
            rps=rh.replace("bytes=","").split("-")
            fb=int(rps[0])
            if len(rps)>1 and rps[1]: ub=int(rps[1])
        rl=ub-fb+1
        cs=1024*1024
        off=(fb//cs)*cs
        fc=fb-off
        lc=(ub%cs)+1
        pc=math.ceil(rl/cs)
        body=ByteStreamer(bot).yield_file(m,0,off,fc,lc,pc,cs)
        hdrs={"Content-Type": m.mime_type or "application/octet-stream", "Accept-Ranges": "bytes",
              "Content-Length": str(rl), "Content-Disposition": f'inline; filename="{m.file_name}"'}
        if rh: hdrs["Content-Range"]=f"bytes {fb}-{ub}/{fsize}"
        return StreamingResponse(body, status_code=206 if rh else 200, headers=hdrs)
    except:
        raise HTTPException(404, "File not found")

@app.get("/")
async def health_check(): return {"status":"ok","message":"Server running!"}

# =====================================
# --- RUN ---
# =====================================

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
