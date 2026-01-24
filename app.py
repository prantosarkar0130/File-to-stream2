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

# Project ki dusri files se import karo
from config import Config
from database import db

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
    except Exception as e:
        print(f"Startup Error: {traceback.format_exc()}")
    yield
    if bot.is_initialized:
        await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}

# --- MULTI-CLIENT SETUP ---
async def start_client(client_id, bot_token):
    try:
        client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=bot_token, no_updates=True, in_memory=True).start()
        work_loads[client_id] = 0
        multi_clients[client_id] = client
    except Exception as e: print(f"Client {client_id} error: {e}")

async def initialize_clients():
    tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
    tasks = [start_client(i, token) for i, token in tokens.items()]
    await asyncio.gather(*tasks)

def get_readable_file_size(s):
    if not s: return '0B'
    for unit in ['B','KB','MB','GB']:
        if s < 1024: return f"{s:.2f} {unit}"
        s /= 1024

def mask_filename(name: str):
    if not name: return "File"
    masked = ''.join(c if (i % 3 == 0) else '*' for i, c in enumerate(name.split('.')[0]))
    return f"{masked}.{name.split('.')[-1]}"

# =====================================================================================
# --- UPDATED: HANDLE FILE UPLOAD (DIRECT LINK + ID SYNC) ---
# =====================================================================================

async def handle_file_upload(message: Message):
    try:
        media = message.document or message.video or message.audio
        if not media: return
        
        # Database check for duplicate
        existing = await db.collection.find_one({'file_unique_id': media.file_unique_id})
        
        if existing:
            unique_id = existing['_id']
            msg_id = existing['message_id']
        else:
            sent = await message.copy(chat_id=Config.STORAGE_CHANNEL)
            unique_id = secrets.token_urlsafe(8)
            msg_id = sent.id
            await db.collection.insert_one({
                '_id': unique_id, 
                'message_id': msg_id,
                'file_unique_id': media.file_unique_id
            })
        
        # URL Formatting
        base_url = Config.BASE_URL.rstrip('/')
        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', media.file_name or "file")
        
        verify_link = f"https://t.me/{Config.BOT_USERNAME}?start=verify_{unique_id}"
        direct_link = f"{base_url}/dl/{msg_id}/{clean_name}"
        
        reply_text = (
            f"**✅ File Successfully Processed!**\n\n"
            f"**1️⃣ User/Verification Link:**\n`{verify_link}`\n\n"
            f"**2️⃣ Direct Stream Link (For ArtPlayer):**\n`{direct_link}`\n\n"
            f"__Tap on the link to copy it.__"
        )
        await message.reply_text(reply_text, quote=True)
        
    except Exception as e:
        print(traceback.format_exc())
        await message.reply_text("Error processing file.")

# --- HANDLERS & ROUTES ---

@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    if len(m.command) > 1 and m.command[1].startswith("verify_"):
        uid = m.command[1].split("_", 1)[1]
        if Config.FORCE_SUB_CHANNEL:
            try: await c.get_chat_member(Config.FORCE_SUB_CHANNEL, m.from_user.id)
            except UserNotParticipant:
                btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{str(Config.FORCE_SUB_CHANNEL).replace('@','')}芽")]]
                return await m.reply_text("Join channel first!", reply_markup=InlineKeyboardMarkup(btn))
        await m.reply_text(f"**Verification Success!**\n\nLink: `{Config.BASE_URL}/show/{uid}`")
    else:
        await m.reply_text("Send me a file!")

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, m): await handle_file_upload(m)

@app.get("/")
async def root(): return {"status": "running"}

@app.get("/show/{uid}", response_class=HTMLResponse)
async def show(request: Request, uid: str):
    return templates.TemplateResponse("show.html", {"request": request})

# --- STREAMING ENGINE ---
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
            for _ in range(pc):
                r=await ms.invoke(raw.functions.upload.GetFile(location=loc,offset=o,limit=cs),retries=2)
                if not r.bytes: break
                yield r.bytes[fc:lc] if pc==1 else (r.bytes[fc:] if _==0 else (r.bytes[:lc] if _==pc-1 else r.bytes))
                o+=cs
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def dl(r:Request, mid:int, fname:str):
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c]=st
    msg = await c.get_messages(Config.STORAGE_CHANNEL, mid)
    m = msg.document or msg.video or msg.audio
    fsize = m.file_size; rh = r.headers.get("Range",""); fb,ub = 0, fsize-1
    if rh:
        p = rh.replace("bytes=","").split("-"); fb=int(p[0])
        if p[1]: ub=int(p[1])
    rl = ub-fb+1; cs=1024*1024; off=(fb//cs)*cs; fc=fb-off; lc=(ub%cs)+1; pc=math.ceil(rl/cs)
    headers = {"Content-Type": m.mime_type or "video/mp4", "Accept-Ranges": "bytes", "Content-Length": str(rl)}
    if rh: headers["Content-Range"] = f"bytes {fb}-{ub}/{fsize}"
    return StreamingResponse(st.yield_file(FileId.decode(m.file_id),idx,off,fc,lc,pc,cs), status_code=206 if rh else 200, headers=headers)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
