# app.py (Final Streaming-Ready Version – CLEAN & STABLE)

import os
import secrets
import math
from contextlib import asynccontextmanager
from urllib.parse import quote

from pyrogram import Client, filters, enums, raw
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
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
bot = Client(
    "StreamBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    in_memory=True,
)

multi_clients = {}
work_loads = {}
class_cache = {}

templates = Jinja2Templates(directory="templates")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    await bot.stop()


app = FastAPI(lifespan=lifespan)

# ==============================================
# HELPERS
# ==============================================
def sanitize_filename(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c in (".", "_", "-")).strip()


# ==============================================
# BOT HANDLERS
# ==============================================
waiting_for_name = {}


@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, message: Message):
    await message.reply_text("👋 Send me any video/file")


@bot.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def file_handler(_, message: Message):
    media = message.document or message.video or message.audio

    waiting_for_name[message.from_user.id] = message
    await message.reply_text("📝 Send file name:")


@bot.on_message(filters.private & filters.text)
async def process_name(_, message: Message):
    uid = message.from_user.id
    if uid not in waiting_for_name:
        return

    orig_msg = waiting_for_name.pop(uid)
    media = orig_msg.document or orig_msg.video or orig_msg.audio

    name = message.text.replace(" ", "_")
    ext = os.path.splitext(media.file_name or "video.mkv")[1] or ".mkv"
    final_name = f"[Moviedekhobd.rf.gd]{name}{ext}"

    sent = await orig_msg.copy(
        chat_id=int(Config.STORAGE_CHANNEL),
        caption=final_name,
    )

    direct = f"{Config.BASE_URL}/dl/{sent.id}/{quote(sanitize_filename(final_name))}"
    await message.reply_text(f"✅ Link:\n{direct}")


# ==============================================
# STREAM ENGINE
# ==============================================
class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client

    async def yield_file(self, f, i, offset, fc, lc, pc, cs):
        work_loads[i] += 1
        try:
            ms = self.client.media_sessions.get(f.dc_id)
            if not ms:
                auth = await Auth(
                    self.client, f.dc_id, await self.client.storage.test_mode()
                ).create()
                ms = Session(
                    self.client,
                    f.dc_id,
                    auth,
                    await self.client.storage.test_mode(),
                    is_media=True,
                )
                await ms.start()
                self.client.media_sessions[f.dc_id] = ms

            loc = raw.types.InputDocumentFileLocation(
                id=f.media_id,
                access_hash=f.access_hash,
                file_reference=f.file_reference,
                thumb_size=f.thumbnail_size,
            )

            for _ in range(pc):
                r = await ms.invoke(
                    raw.functions.upload.GetFile(location=loc, offset=offset, limit=cs)
                )
                if not r.bytes:
                    break

                yield r.bytes[fc:lc] if pc == 1 else r.bytes
                offset += cs
        finally:
            work_loads[i] -= 1


# ==============================================
# STREAM ROUTE
# ==============================================
@app.get("/dl/{mid}/{fname}")
async def stream_media(request: Request, mid: int, fname: str):
    try:
        idx = min(work_loads, key=work_loads.get)
        client = multi_clients[idx]
        streamer = class_cache.get(client) or ByteStreamer(client)
        class_cache[client] = streamer

        msg = await client.get_messages(int(Config.STORAGE_CHANNEL), mid)
        media = msg.document or msg.video or msg.audio
        fid = FileId.decode(media.file_id)

        size = media.file_size
        range_h = request.headers.get("Range")

        start, end = 0, size - 1
        if range_h:
            start = int(range_h.replace("bytes=", "").split("-")[0])

        length = end - start + 1
        cs = 1024 * 256
        off = (start // cs) * cs
        fc = start - off
        lc = (end % cs) + 1
        pc = math.ceil(length / cs)

        headers = {
            "Content-Type": "video/mp4",
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
        }

        if range_h:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
            headers["Content-Length"] = str(length)

        async def iterator():
            async for chunk in streamer.yield_file(fid, idx, off, fc, lc, pc, cs):
                yield chunk

        return StreamingResponse(
            iterator(),
            status_code=206 if range_h else 200,
            headers=headers,
        )

    except Exception:
        raise HTTPException(404)


# ==============================================
# OPTIONS + HEAD (CRITICAL)
# ==============================================
@app.options("/dl/{mid}/{fname}")
async def options_dl(mid: int, fname: str):
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
        },
    )


@app.head("/dl/{mid}/{fname}")
async def head_dl(mid: int, fname: str):
    return JSONResponse(
        content={},
        headers={
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ==============================================
# SHOW PAGE
# ==============================================
@app.get("/show/{uid}", response_class=HTMLResponse)
async def show_page(request: Request, uid: str):
    return templates.TemplateResponse("show.html", {"request": request})


# ==============================================
# HEALTH
# ==============================================
@app.get("/")
async def health():
    return {"status": "ok"}


# ==============================================
# RUN
# ==============================================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000)
