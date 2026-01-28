import os, asyncio, secrets, traceback, uvicorn, math
from contextlib import asynccontextmanager
from pyrogram import Client, filters, raw
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.file_id import FileId
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from config import Config
from database import db

# Global variables for multi-bot and workload
multi_clients = {}; work_loads = {}; waiting_for_name = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        # Start Main Bot
        await bot.start()
        multi_clients[0] = bot; work_loads[0] = 0
        
        # Start Multi Bots from ENV
        tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
        for i, token in tokens.items():
            try:
                c = await Client(name=str(i), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
                multi_clients[i] = c; work_loads[i] = 0
            except: pass
        print("✨ Bot & Multi-Clients are Live!")
    except: print(traceback.format_exc())
    yield
    await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)

def get_readable_size(size):
    for unit in ['B','KB','MB','GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024

# --- BOT HANDLERS (Same logic as yours) ---
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    await message.reply_text(f"👋 **Hello {message.from_user.first_name}!**\n📂 Send me any video for high-speed streaming.")

@bot.on_message(filters.private & (filters.document | filters.video))
async def handle_file(client, message):
    media = message.document or message.video
    ex = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if ex:
        u_id = ex["_id"]; m_id = ex["message_id"]; f_name = ex.get("file_name", "video.mkv")
        d_link = f"{Config.BASE_URL}/dl/{m_id}/{f_name}"
        return await message.reply_text(f"✅ **Existing File!**\n\n🔗 Link: `{d_link}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]]))
    
    waiting_for_name[message.from_user.id] = message
    await message.reply_text("📝 **Send a Name for the file:**")

@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name(client, message):
    uid = message.from_user.id
    if uid not in waiting_for_name: return
    orig = waiting_for_name.pop(uid)
    media = orig.document or orig.video
    ext = os.path.splitext(media.file_name or ".mkv")[1] or ".mkv"
    final_name = f"moviedekhobd.rf.gd_{message.text.replace(' ', '_')}_moviedekhobd.rf.gd{ext}"
    
    sts = await message.reply_text("🚀 **Uploading...**")
    try:
        sent = await bot.send_video(int(Config.STORAGE_CHANNEL), media.file_id, file_name=final_name) if orig.video else await bot.send_document(int(Config.STORAGE_CHANNEL), media.file_id, file_name=final_name)
        u_id = secrets.token_urlsafe(8)
        await db.collection.insert_one({"_id": u_id, "message_id": sent.id, "file_unique_id": media.file_unique_id, "file_name": final_name})
        await sts.delete()
        await orig.reply_text(f"✅ **Success!**\n\n🔗 Link: `{Config.BASE_URL}/dl/{sent.id}/{final_name}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]]))
    except: await message.reply_text("❌ Failed!")

# --- FAST STREAMING LOGIC ---
async def data_generator(f_id, idx, offset, limit, chunk_size):
    client = multi_clients.get(idx, bot)
    work_loads[idx] += 1
    try:
        session = client.session
        loc = raw.types.InputDocumentFileLocation(id=f_id.media_id, access_hash=f_id.access_hash, file_reference=f_id.file_reference, thumb_size=f_id.thumbnail_size)
        while limit > 0:
            r = await session.invoke(raw.functions.upload.GetFile(location=loc, offset=offset, limit=min(limit, chunk_size)))
            if not r or not r.bytes: break
            yield r.bytes
            offset += len(r.bytes); limit -= len(r.bytes)
            await asyncio.sleep(0) # Let the event loop breathe
    finally: work_loads[idx] -= 1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r: Request, mid: int, fname: str):
    idx = min(work_loads, key=work_loads.get)
    try:
        msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video
        f_id = FileId.decode(m.file_id)
        range_h = r.headers.get("Range", "")
        start = int(range_h.replace("bytes=", "").split("-")[0]) if range_h else 0
        
        return StreamingResponse(
            data_generator(f_id, idx, start, m.file_size - start, 1024*512),
            status_code=206 if range_h else 200,
            headers={
                "Content-Type": "video/mp4",
                "Accept-Ranges": "bytes",
                "Content-Length": str(m.file_size - start),
                "Content-Range": f"bytes {start}-{m.file_size-1}/{m.file_size}"
            }
        )
    except: raise HTTPException(404)

@app.get("/show/{unique_id}", response_class=HTMLResponse)
async def show_page(request: Request, unique_id: str):
    return templates.TemplateResponse("show.html", {"request": request, "id": unique_id})

@app.get("/api/file/{unique_id}")
async def get_api_data(unique_id: str):
    data = await db.collection.find_one({"_id": unique_id})
    msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), data["message_id"])
    media = msg.document or msg.video
    return {"file_name": data.get("file_name", "video.mkv"), "file_size": get_readable_size(media.file_size), "is_media": True, "direct_dl_link": f"{Config.BASE_URL}/dl/{data['message_id']}/video.mkv"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
