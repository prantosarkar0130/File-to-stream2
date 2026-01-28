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

# লজিক ঠিক রাখার জন্য গ্লোবাল ভেরিয়েবল
multi_clients = {}; work_loads = {}; waiting_for_name = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        await bot.start()
        multi_clients[0] = bot; work_loads[0] = 0
        tokens = {c+1: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}
        for i, token in tokens.items():
            try:
                c = await Client(name=str(i), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
                multi_clients[i] = c; work_loads[i] = 0
            except: pass
        print("✅ Database & Multi-Bots Connected!")
    except: print(traceback.format_exc())
    yield

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

bot = Client("SimpleStreamBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, in_memory=True)

# --- BOT HANDLERS ---
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(c, m):
    await m.reply_text(f"👋 **Hello {m.from_user.first_name}!**\nSend me a video for high-speed link.")

@bot.on_message(filters.private & (filters.document | filters.video))
async def handle_file(c, m):
    media = m.document or m.video
    # ডাটাবেস চেক - এই লজিকটি আগের মতোই আছে
    ex = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if ex:
        u_id = ex["_id"]; m_id = ex["message_id"]
        f_name = ex.get("file_name", "video.mkv").replace(" ", "_")
        d_link = f"{Config.BASE_URL}/dl/{m_id}/{f_name}"
        return await m.reply_text(f"✅ **Existing File!**\n\n🔗 **Link:** `{d_link}`", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]]))
    
    waiting_for_name[m.from_user.id] = m
    await m.reply_text("📝 **Send a Name for this video:**")

@bot.on_message(filters.private & filters.text & ~filters.command("start"))
async def process_name(c, m):
    uid = m.from_user.id
    if uid not in waiting_for_name: return
    orig = waiting_for_name.pop(uid)
    media = orig.document or orig.video
    ext = os.path.splitext(media.file_name or ".mkv")[1] or ".mkv"
    
    # আপনার ফরম্যাট অনুযায়ী নাম
    user_input = m.text.replace(" ", "_")
    final_name = f"moviedekhobd.rf.gd_{user_input}_moviedekhobd.rf.gd{ext}"
    
    sts = await m.reply_text("🚀 **Processing...**")
    try:
        sc = int(Config.STORAGE_CHANNEL)
        sent = await bot.send_video(sc, media.file_id, file_name=final_name) if orig.video else await bot.send_document(sc, media.file_id, file_name=final_name)
        u_id = secrets.token_urlsafe(8)
        # ডাটাবেসে সেভ করা হচ্ছে
        await db.collection.insert_one({"_id": u_id, "message_id": sent.id, "file_unique_id": media.file_unique_id, "file_name": final_name})
        
        d_link = f"{Config.BASE_URL}/dl/{sent.id}/{final_name}"
        await sts.delete()
        await m.reply_text(f"✅ **Success!**\n\n🔗 **Link:** `{d_link}`", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥️ Watch Online", url=f"{Config.BASE_URL}/show/{u_id}")]]))
    except: await m.reply_text("❌ Error processing file.")

# --- HIGH SPEED STREAMING ---
async def data_generator(f_id, client_idx, offset, limit, chunk_size):
    client = multi_clients[client_idx]
    work_loads[client_idx] += 1
    try:
        session = client.session
        loc = raw.types.InputDocumentFileLocation(id=f_id.media_id, access_hash=f_id.access_hash, file_reference=f_id.file_reference, thumb_size=f_id.thumbnail_size)
        while limit > 0:
            r = await session.invoke(raw.functions.upload.GetFile(location=loc, offset=offset, limit=min(limit, chunk_size)))
            if not r or not r.bytes: break
            yield r.bytes
            offset += len(r.bytes); limit -= len(r.bytes)
            await asyncio.sleep(0) 
    finally: work_loads[client_idx] -= 1

@app.get("/dl/{mid}/{fname}")
async def stream_media(r: Request, mid: int, fname: str):
    idx = min(work_loads, key=work_loads.get)
    try:
        msg = await multi_clients[0].get_messages(int(Config.STORAGE_CHANNEL), mid)
        m = msg.document or msg.video
        f_id = FileId.decode(m.file_id)
        range_h = r.headers.get("Range", "")
        start = int(range_h.replace("bytes=", "").split("-")[0]) if range_h else 0
        return StreamingResponse(data_generator(f_id, idx, start, m.file_size - start, 1024*512), status_code=206 if range_h else 200,
            headers={"Content-Type": "video/mp4", "Accept-Ranges": "bytes", "Content-Length": str(m.file_size - start), "Content-Range": f"bytes {start}-{m.file_size-1}/{m.file_size}"})
    except: raise HTTPException(404)

@app.get("/show/{id}", response_class=HTMLResponse)
async def show_page(request: Request, id: str):
    return templates.TemplateResponse("show.html", {"request": request, "id": id})

@app.get("/api/file/{id}")
async def api_file(id: str):
    data = await db.collection.find_one({"_id": id})
    if not data: return JSONResponse({"error": "Not Found"}, status_code=404)
    return {"direct_dl_link": f"{Config.BASE_URL}/dl/{data['message_id']}/{data.get('file_name', 'video.mkv')}", "file_name": data.get("file_name", "video.mkv")}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
