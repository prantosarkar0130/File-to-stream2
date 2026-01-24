import os
import asyncio
import secrets
import traceback
import uvicorn
import re
import math
from contextlib import asynccontextmanager

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pyrogram.file_id import FileId
from pyrogram import raw
from pyrogram.session import Session, Auth

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
    except Exception: print(traceback.format_exc())
    yield
    if bot.is_initialized: await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

bot = Client("StreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)
multi_clients = {}; work_loads = {}; class_cache = {}

async def start_client(c_id, token):
    try:
        client = await Client(name=str(c_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
        work_loads[c_id] = 0
        multi_clients[c_id] = client
    except Exception: pass

async def initialize_clients():
    tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
    tasks = [start_client(i, token) for i, token in tokens.items()]
    await asyncio.gather(*tasks)

def get_size(s):
    if not s: return "0 B"
    for unit in ['B','KB','MB','GB','TB']:
        if s < 1024: return f"{s:.2f} {unit}"
        s /= 1024

def clean_file_name(name: str):
    """লিঙ্ক থেকে সব জঞ্জাল পরিষ্কার করে নিখুঁত .mp4 লিঙ্ক বানায়"""
    if not name: return f"video_{secrets.token_hex(4)}.mp4"
    # শুধু অক্ষর, সংখ্যা এবং ডট রেখে বাকি সব আন্ডারস্কোর করা
    clean = re.sub(r'[^a-zA-Z0-9.]', '_', name)
    # একাধিক আন্ডারস্কোর (____) থাকলে ১টি করা
    clean = re.sub(r'_+', '_', clean)
    # ডট এর আশেপাশের আন্ডারস্কোর কাটা এবং ক্লিন করা
    clean = clean.strip('_').replace('._', '.').replace('_.', '.')
    
    # এক্সটেনশন চেক
    if not clean.lower().endswith(('.mp4', '.mkv', '.mp3', '.webm')):
        clean += ".mp4"
    return clean[:60]

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file_upload(_, m: Message):
    try:
        media = m.document or m.video or m.audio
        # ফাইল চেক (একই ফাইল হলে আগের আইডি ব্যবহার করবে)
        ex = await db.collection.find_one({'file_unique_id': media.file_unique_id})
        if ex:
            uid, mid = ex['_id'], ex['message_id']
        else:
            sent = await m.copy(chat_id=Config.STORAGE_CHANNEL)
            uid, mid = secrets.token_urlsafe(8), sent.id
            await db.collection.insert_one({'_id': uid, 'message_id': mid, 'file_unique_id': media.file_unique_id})
        
        base = Config.BASE_URL.rstrip('/')
        fname = clean_file_name(media.file_name)
        stream_link = f"{base}/dl/{mid}/{fname}"
        page_link = f"{base}/show/{uid}"
        
        reply = (
            f"🎬 **Movie:** `{media.file_name}`\n"
            f"⚖️ **Size:** `{get_size(media.file_size)}`"
        )
        btn = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Watch Online", url=page_link)],
            [InlineKeyboardButton("🔗 Direct Link", url=stream_link)]
        ])
        await m.reply_text(reply, reply_markup=btn, quote=True)
    except Exception: print(traceback.format_exc())

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
                r=await ms.invoke(raw.functions.upload.GetFile(location=loc,offset=o,limit=cs),retries=3)
                if not r.bytes: break
                if pc==1: yield r.bytes[fc:lc]
                elif cp==1: yield r.bytes[fc:]
                elif cp==pc: yield r.bytes[:lc]
                else: yield r.bytes
                o+=cs
        finally: work_loads[i]-=1

@app.get("/dl/{mid}/{fname}")
async def dl(r:Request, mid:int, fname:str):
    if not multi_clients: raise HTTPException(503, "Bots not ready")
    idx = min(work_loads, key=work_loads.get); c = multi_clients[idx]
    st = class_cache.get(c) or ByteStreamer(c); class_cache[c]=st
    try:
        msg = await c.get_messages(Config.STORAGE_CHANNEL, mid)
        m = msg.document or msg.video or msg.audio
        fsize = m.file_size; rh = r.headers.get("Range",""); fb,ub = 0, fsize-1
        if rh:
            p = rh.replace("bytes=","").split("-"); fb=int(p[0])
            if p[1]: ub=int(p[1])
        rl, cs = ub-fb+1, 1024*1024
        off, fc, lc, pc = (fb//cs)*cs, fb-(fb//cs)*cs, (ub%cs)+1, math.ceil(rl/cs)
        headers = {
            "Content-Type": m.mime_type or "video/mp4",
            "Accept-Ranges": "bytes",
            "Content-Length": str(rl),
            "Content-Disposition": f'inline; filename="{fname}"'
        }
        if rh: headers["Content-Range"] = f"bytes {fb}-{ub}/{fsize}"
        return StreamingResponse(st.yield_file(FileId.decode(m.file_id),idx,off,fc,lc,pc,cs), status_code=206 if rh else 200, headers=headers)
    except Exception: raise HTTPException(404, "File not found")

@app.get("/api/file/{uid}")
async def get_info(uid:str):
    m_id = await db.get_link(uid)
    msg = await bot.get_messages(Config.STORAGE_CHANNEL, m_id)
    m = msg.document or msg.video or msg.audio
    fname = clean_file_name(m.file_name)
    dl_u = f"{Config.BASE_URL}/dl/{m_id}/{fname}"
    return {
        "file_name": m.file_name, 
        "file_size": get_size(m.file_size), 
        "direct_dl_link": dl_u, 
        "vlc_player_link": f"vlc://{dl_u}", 
        "mx_player_link": f"intent:{dl_u}#Intent;action=android.intent.action.VIEW;type=video/*;end"
    }

@app.get("/show/{uid}", response_class=HTMLResponse)
async def show_page(request: Request, uid: str): 
    return templates.TemplateResponse("show.html", {"request": request})

@app.get("/")
async def root(): return {"status": "Live"}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
