import os, asyncio, secrets, traceback, uvicorn, re, math
from contextlib import asynccontextmanager
from pyrogram import Client, filters, raw
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.file_id import FileId
from pyrogram.session import Session, Auth
from pyrogram.errors import FloodWait
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from config import Config
from database import db

waiting_for_name = {}
work_load = 0
stream_cache = {}

# ─────────────────── LIFESPAN ───────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    try:
        try:
            await bot.start()
        except FloodWait as e:
            print(f"⏳ FloodWait {e.value}s")
            await asyncio.sleep(e.value)
            await bot.start()

        me = await bot.get_me()
        Config.BOT_USERNAME = me.username
        print(f"✅ Bot @{Config.BOT_USERNAME} started")

    except Exception:
        print(traceback.format_exc())

    yield

    if bot.is_initialized:
        await bot.stop()

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)

bot = Client(
    "SimpleStreamBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    in_memory=True
)

def get_readable_size(size):
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {u}"
        size /= 1024

# ─────────────────── BOT ───────────────────
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, m):
    await m.reply_text(
        f"👋 Hello {m.from_user.first_name}\n\nSend any video/file to get stream link"
    )

@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file(_, m):
    media = m.document or m.video or m.audio

    ex = await db.collection.find_one({"file_unique_id": media.file_unique_id})
    if ex:
        uid = ex["_id"]
        fname = ex["file_name"]
        link = f"{Config.BASE_URL}/dl/{ex['message_id']}/{fname}"
        return await m.reply_text(
            f"✅ **Already Exists**\n\n🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("▶️ Watch", url=f"{Config.BASE_URL}/show/{uid}")]]
            )
        )

    waiting_for_name[m.from_user.id] = m
    await m.reply_text("📝 **Send file name**")

@bot.on_message(filters.private & filters.text)
async def process_name(_, m):
    uid = m.from_user.id
    if uid not in waiting_for_name:
        return

    orig = waiting_for_name.pop(uid)
    media = orig.document or orig.video or orig.audio

    ext = os.path.splitext(media.file_name or ".mp4")[1]
    name = re.sub(r"\s+", "_", m.text)
    final_name = f"moviedekhobd_{name}{ext}"

    try:
        # 🔥 FIX: media type safe upload
        if orig.video:
            msg = await bot.send_video(
                int(Config.STORAGE_CHANNEL),
                media.file_id,
                file_name=final_name,
                caption=final_name
            )
        else:
            msg = await bot.send_document(
                int(Config.STORAGE_CHANNEL),
                media.file_id,
                file_name=final_name,
                caption=final_name
            )

        u_id = secrets.token_urlsafe(8)

        await db.collection.insert_one({
            "_id": u_id,
            "message_id": msg.id,
            "file_unique_id": media.file_unique_id,
            "file_name": final_name
        })

        link = f"{Config.BASE_URL}/dl/{msg.id}/{final_name}"

        await orig.reply_text(
            f"✅ **Upload Successful**\n\n🔗 `{link}`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("▶️ Watch", url=f"{Config.BASE_URL}/show/{u_id}")]]
            )
        )

    except Exception as e:
        print(e)
        await orig.reply_text("❌ Upload failed")

# ─────────────────── STREAM ENGINE ───────────────────
class ByteStreamer:
    def __init__(self, client):
        self.client = client

    async def get_session(self, dc_id):
        if dc_id not in self.client.media_sessions:
            auth = await Auth(self.client, dc_id, False).create()
            session = Session(self.client, dc_id, auth, False, is_media=True)
            await session.start()
            self.client.media_sessions[dc_id] = session
        return self.client.media_sessions[dc_id]

    async def stream(self, file, start, end, chunk):
        session = await self.get_session(file.dc_id)
        offset = start

        while offset <= end:
            limit = min(chunk, end - offset + 1)
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=raw.types.InputDocumentFileLocation(
                        id=file.media_id,
                        access_hash=file.access_hash,
                        file_reference=file.file_reference,
                        thumb_size=""
                    ),
                    offset=offset,
                    limit=limit
                )
            )
            if not r or not r.bytes:
                break
            yield r.bytes
            offset += len(r.bytes)

# ─────────────────── STREAM ROUTE ───────────────────
@app.get("/dl/{mid}/{fname}")
async def stream_media(req: Request, mid: int, fname: str):
    global work_load
    work_load += 1

    try:
        msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), mid)
        media = msg.document or msg.video
        file = FileId.decode(media.file_id)

        size = media.file_size
        range_header = req.headers.get("range")

        start = 0
        end = size - 1

        if range_header:
            start, end = range_header.replace("bytes=", "").split("-")
            start = int(start)
            end = int(end) if end else size - 1

        chunk = 1024 * 1024  # 1MB
        streamer = stream_cache.get(bot) or ByteStreamer(bot)
        stream_cache[bot] = streamer

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Type": media.mime_type or "video/mp4",
            "Content-Length": str(end - start + 1),
            "Content-Range": f"bytes {start}-{end}/{size}",
        }

        return StreamingResponse(
            streamer.stream(file, start, end, chunk),
            status_code=206 if range_header else 200,
            headers=headers
        )
    finally:
        work_load -= 1

# ─────────────────── PAGES / API ───────────────────
@app.get("/show/{uid}", response_class=HTMLResponse)
async def show_page(req: Request, uid: str):
    return templates.TemplateResponse("show.html", {"request": req, "id": uid})

@app.get("/api/file/{uid}")
async def api(uid: str):
    data = await db.collection.find_one({"_id": uid})
    if not data:
        return JSONResponse({"error": "404"}, status_code=404)

    msg = await bot.get_messages(int(Config.STORAGE_CHANNEL), data["message_id"])
    media = msg.document or msg.video

    return {
        "file_name": data["file_name"],
        "file_size": get_readable_size(media.file_size),
        "direct_dl": f"{Config.BASE_URL}/dl/{data['message_id']}/{data['file_name']}"
    }

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=10000)
