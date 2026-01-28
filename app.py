import os
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

from motor.motor_asyncio import AsyncIOMotorClient

# ===================== CONFIG =====================

API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "streamdb"
COLL_NAME = "files"

STREAM_DOMAIN = os.environ.get(
    "STREAM_DOMAIN",
    "https://file-to-stream2.onrender.com"
)

# ===================== DB =====================

mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo[DB_NAME]
files = db[COLL_NAME]

# ===================== BOT =====================

bot = Client(
    "streambot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=1,          # 🔒 single bot, multi off
    in_memory=True
)

# ===================== FASTAPI =====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to the database...")
    await mongo.admin.command("ping")
    print("✅ Database connection established.")

    await bot.start()
    me = await bot.get_me()
    print(f"✅ Bot @{me.username} started")

    yield

    await bot.stop()
    mongo.close()

app = FastAPI(lifespan=lifespan)

# ===================== KEEP ALIVE =====================

@app.get("/")
async def root():
    return {"status": "alive"}

# ===================== BOT HANDLERS =====================

@bot.on_message(filters.command("start"))
async def start_cmd(_, msg: Message):
    await msg.reply_text(
        "👋 Video পাঠাও\n"
        "আমি direct stream link বানিয়ে দেবো"
    )

@bot.on_message(filters.video | filters.document)
async def handle_media(_, msg: Message):
    media = msg.video or msg.document

    if not media:
        return

    file_id = media.file_id
    file_name = media.file_name or f"{media.file_unique_id}.mp4"

    # আগে DB তে আছে কিনা check
    exist = await files.find_one({"file_id": file_id})
    if exist:
        link = f"{STREAM_DOMAIN}/dl/{exist['msg_id']}/{exist['file_name']}"
        await msg.reply_text(f"♻️ Already added\n\n🔗 {link}")
        return

    # storage channel = Saved Messages
    sent = await bot.send_video(
        "me",
        video=file_id,
        file_name=file_name
    )

    await files.insert_one({
        "msg_id": sent.id,
        "file_id": file_id,
        "file_name": file_name
    })

    link = f"{STREAM_DOMAIN}/dl/{sent.id}/{file_name}"
    await msg.reply_text(f"✅ Uploaded\n\n🔗 {link}")

# ===================== STREAM ROUTE =====================

@app.api_route("/dl/{msg_id}/{fname}", methods=["GET", "HEAD"])
async def stream(req: Request, msg_id: int, fname: str):

    # HEAD request fix (VERY IMPORTANT)
    if req.method == "HEAD":
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Type": "video/mp4"
            }
        )

    data = await files.find_one({"msg_id": msg_id})
    if not data:
        raise HTTPException(404, "File not found")

    try:
        msg = await bot.get_messages("me", msg_id)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        msg = await bot.get_messages("me", msg_id)

    if not msg or not msg.video:
        raise HTTPException(404, "Invalid media")

    file_size = msg.video.file_size
    chunk = 512 * 1024   # 512KB (Render safe)

    range_header = req.headers.get("range")
    start = 0
    end = file_size - 1

    if range_header:
        start = int(range_header.split("=")[1].split("-")[0])

    async def generator():
        async for part in bot.stream_media(
            msg.video,
            offset=start,
            limit=chunk
        ):
            yield part

    headers = {
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
        "Cache-Control": "public, max-age=86400"
    }

    return StreamingResponse(
        generator(),
        status_code=206 if range_header else 200,
        headers=headers
    )
