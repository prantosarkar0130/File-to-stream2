# app.py (MODIFIED WITH DB CHECK & DIRECT LINK)

import os
import asyncio
import secrets
import traceback
import uvicorn
import re
import logging
from contextlib import asynccontextmanager

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from pyrogram.errors import FloodWait, UserNotParticipant
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pyrogram.file_id import FileId
from pyrogram import raw
from pyrogram.session import Session, Auth
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import math

from config import Config
from database import db

# --- SETUP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        await bot.start()
        me = await bot.get_me()
        Config.BOT_USERNAME = me.username
        multi_clients[0] = bot
        work_loads[0] = 0
        await initialize_clients()
        await bot.get_chat(Config.STORAGE_CHANNEL)
        try: await cleanup_channel(bot)
        except: pass
    except Exception: print(traceback.format_exc())
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}

# --- MULTI-CLIENT STARTUP ---
async def start_client(client_id, bot_token):
    try:
        client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=bot_token, no_updates=True, in_memory=True).start()
        work_loads[client_id] = 0
        multi_clients[client_id] = client
    except Exception as e: print(f"Error starting client {client_id}: {e}")

async def initialize_clients():
    tokens = {c + 1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
    tasks = [start_client(i, token) for i, token in tokens.items()]
    await asyncio.gather(*tasks)

# --- HELPERS ---
def get_readable_file_size(size_in_bytes):
    if not size_in_bytes: return '0B'
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024: return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

# --- HANDLERS ---
@bot.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    if len(message.command) > 1 and message.command[1].startswith("verify_"):
        unique_id = message.command[1].split("_", 1)[1]
        if Config.FORCE_SUB_CHANNEL:
            try: await client.get_chat_member(Config.FORCE_SUB_CHANNEL, message.from_user.id)
            except UserNotParticipant:
                btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{str(Config.FORCE_SUB_CHANNEL).replace('@', '')}")],
                       [InlineKeyboardButton("✅ Joined", url=f"https://t.me/{Config.BOT_USERNAME}?start={message.command[1]}")]]
                return await message.reply_text("**Join our channel to get the link!**", reply_markup=InlineKeyboardMarkup(btn))
        
        final_link = f"{Config.BASE_URL}/show/{unique_id}"
        await message.reply_text(f"✅ Verification Successful!\n\n`{final_link}`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Link", url=final_link)]]))
    else:
        await message.reply_text(f"👋 Hello {message.from_user.first_name}!\nSend me any file to get a stream link.")

async def handle_file_upload(message: Message):
    try:
        media = message.document or message.video or message.audio
        if not media: return

        # --- DATABASE CHECK (PREVENT DUPLICATE) ---
        ex = await db.collection.find_one({'file_unique_id': media.file_unique_id})
        if ex:
            unique_id, msg_id = ex['_id'], ex['message_id']
        else:
            sent = await message.copy(chat_id=Config.STORAGE_CHANNEL)
            unique_id, msg_id = secrets.token_urlsafe(8), sent.id
            await db.collection.insert_one({'_id': unique_id, 'message_id': msg_id, 'file_unique_id': media.file_unique_id})
        
        # --- LINKS ---
        verify_link = f"https://t.me/{Config.BOT_USERNAME}?start=verify_{unique_id}"
        direct_link = f"{Config.BASE_URL}/dl/{msg_id}/video.mp4"
        
        reply_text = (
            f"✅ **File Uploaded!**\n\n"
            f"🎬 **Name:** `{media.file_name}`\n"
            f"⚖️ **Size:** `{get_readable_file_size(media.file_size)}`\n\n"
            f"🔗 **Direct Stream Link:**\n`{direct_link}`"
        )
        
        button = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Get Watch Link", url=verify_link)],
            [InlineKeyboardButton("🌐 Direct Stream", url=direct_link)]
        ])
        
        await message.reply_text(reply_text, reply_markup=button, quote=True)
    except Exception: print(traceback.format_exc())

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, message: Message):
    await handle_file_upload(message)

# --- WEB SERVER & STREAMING (UNCHANGED LOGIC) ---
@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request})

@app.get("/api/file/{unique_id}")
async def get_file_api(unique_id: str):
    mid = await db.get_link(unique_id)
    if not mid: raise HTTPException(404)
    msg = await bot.get_messages(Config.STORAGE_CHANNEL, mid)
    media = msg.document or msg.video or msg.audio
    dl = f"{Config.BASE_URL}/dl/{mid}/video.mp4"
    return {"file_name": media.file_name, "file_size": get_readable_file_size(media.file_size), "direct_dl_link": dl}

class ByteStreamer:
    def __init__(self,c): self.client=c
    async def yield_file(self,f,i,o,fc,lc,pc,cs):
        c=self.client; work_loads[i]+=1
        ms=c.media_sessions.get(f.dc_id)
        if not ms:
            if f.dc_id!=await c.storage.dc_id():
                ak=await Auth(c,f.dc_id,await c.storage.test_mode()).create(); ms=Session(c,f.dc_id,ak,await c.storage.test_mode(),is_media=True); await ms.start()
                ea=await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=f.dc_id)); await ms.invoke(raw.functions.auth.ImportAuthorization(id=ea.id,bytes=ea.bytes))
            else: ms=c.session
            c.media_sessions[f.dc_id]=ms
        loc=raw.types.InputDocumentFileLocation(id=f.media_id,access_hash=f.access_hash,file_reference=f.file_reference,thumb_size=f.thumbnail_size)
        try:
            for cp in range(1, pc + 1):
                r=await ms.invoke(raw.functions.upload.GetFile(location=loc,offset=o,limit=cs))
                if not r.bytes: break
                if pc==1: yield r.bytes[fc:lc]
                elif cp==1: yield r.bytes[fc:]
                elif cp==pc: yield r.bytes[:lc]
                else: yield r.bytes
                o+=cs
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r:Request, mid:int, fname:str):
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c]=st
    msg = await c.get_messages(Config.STORAGE_CHANNEL, mid)
    m = msg.document or msg.video or msg.audio
    fsize = m.file_size; rh = r.headers.get("Range",""); fb,ub = 0, fsize-1
    if rh:
        p = rh.replace("bytes=","").split("-"); fb=int(p[0])
        if p[1]: ub=int(p[1])
    rl, cs = ub-fb+1, 1024*1024
    off, fc, lc, pc = (fb//cs)*cs, fb-(fb//cs)*cs, (ub%cs)+1, math.ceil(rl/cs)
    headers = {"Content-Type": m.mime_type or "video/mp4", "Accept-Ranges": "bytes", "Content-Length": str(rl)}
    if rh: headers["Content-Range"] = f"bytes {fb}-{ub}/{fsize}"
    return StreamingResponse(st.yield_file(FileId.decode(m.file_id),idx,off,fc,lc,pc,cs), status_code=206 if rh else 200, headers=headers)

@app.get("/")
async def health(): return {"status": "ok"}

async def cleanup_channel(c): pass # Logic kept for safety

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
