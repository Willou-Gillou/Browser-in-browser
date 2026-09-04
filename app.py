import asyncio
import json
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from playwright.async_api import async_playwright

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(BASE_DIR, "static", "index.html")

DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

START_URL = os.environ.get("START_URL", "https://www.google.com")
VIEWPORT = {"width": 1280, "height": 720}

state = {}


@app.on_event("startup")
async def startup():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = await browser.new_context(user_agent=DESKTOP_UA, viewport=VIEWPORT)
    # Masque les indices classiques de navigateur automatisé (navigator.webdriver
    # notamment), que reCAPTCHA/hCaptcha utilisent pour bloquer la validation.
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )
    page = await context.new_page()
    await page.goto(START_URL)

    state["pw"] = pw
    state["browser"] = browser
    state["context"] = context
    state["page"] = page


@app.on_event("shutdown")
async def shutdown():
    await state["browser"].close()
    await state["pw"].stop()


@app.get("/")
async def index():
    return FileResponse(INDEX_PATH)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    page = state["page"]
    cdp = await state["context"].new_cdp_session(page)

    frame_queue: asyncio.Queue = asyncio.Queue()
    cdp.on("Page.screencastFrame", lambda params: frame_queue.put_nowait(params))

    await cdp.send(
        "Page.startScreencast",
        {
            "format": "jpeg",
            "quality": 80,
            "maxWidth": VIEWPORT["width"],
            "maxHeight": VIEWPORT["height"],
            "everyNthFrame": 1,
        },
    )

    async def sender():
        while True:
            params = await frame_queue.get()
            await websocket.send_text(json.dumps({"type": "frame", "data": params["data"]}))
            await cdp.send("Page.screencastFrameAck", {"sessionId": params["sessionId"]})

    async def receiver():
        while True:
            raw = await websocket.receive_text()
            try:
                await handle_input(page, cdp, json.loads(raw))
            except Exception as e:
                # Une erreur sur une action (ex: goto qui time-out) ne doit
                # jamais couper toute la session WebSocket.
                print(f"handle_input error: {e}")

    sender_task = asyncio.create_task(sender())
    try:
        await receiver()
    except WebSocketDisconnect:
        pass
    finally:
        sender_task.cancel()
        try:
            await cdp.send("Page.stopScreencast")
            await cdp.detach()
        except Exception:
            pass


async def handle_input(page, cdp, msg: dict):
    t = msg.get("type")
    if t == "mousemove":
        await page.mouse.move(msg["x"], msg["y"])
    elif t == "mousedown":
        await page.mouse.move(msg["x"], msg["y"])
        await page.mouse.down()
    elif t == "mouseup":
        await page.mouse.up()
    elif t == "wheel":
        await page.mouse.wheel(msg.get("dx", 0), msg.get("dy", 0))
    elif t == "keydown":
        await page.keyboard.down(msg["key"])
    elif t == "keyup":
        await page.keyboard.up(msg["key"])
    elif t == "goto":
        try:
            await page.goto(msg["url"], timeout=15000, wait_until="domcontentloaded")
        finally:
            # Chrome arrête parfois le screencast lors d'une navigation :
            # on le relance systématiquement pour ne pas rester figé sur
            # la dernière frame de l'ancienne page.
            try:
                await cdp.send(
                    "Page.startScreencast",
                    {
                        "format": "jpeg",
                        "quality": 80,
                        "maxWidth": VIEWPORT["width"],
                        "maxHeight": VIEWPORT["height"],
                        "everyNthFrame": 1,
                    },
                )
            except Exception:
                pass


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 7860)))
