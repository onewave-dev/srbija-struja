import hmac
import logging
import os
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Optional

from fastapi import FastAPI, Request, Response
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from main.grebeshok_game.handlers import build_handler as grebeshok_handler
from main.guess_game.handlers import build_handler as guess_handler

logging.basicConfig(level=logging.INFO)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Угадай число", callback_data="game_guess")],
        [InlineKeyboardButton("Гребешок", callback_data="game_grebeshok")],
    ])
    await update.message.reply_text("Выберите игру:", reply_markup=keyboard)


async def webhook_watchdog(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        desired = _desired_webhook_url()
        info = await context.bot.get_webhook_info()
        mismatch = bool(desired) and (info.url != desired)
        has_error = bool(getattr(info, "last_error_message", None))
        if mismatch or has_error:
            await context.bot.set_webhook(
                url=desired or info.url,
                allowed_updates=[],
                drop_pending_updates=False,
                secret_token=os.environ.get("WEBHOOK_SECRET") or None,
            )
    except TelegramError:
        logging.exception("Webhook watchdog failed")


def build_ptb_app() -> Application:
    token = os.environ["BOT_TOKEN"]
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(guess_handler())
    app.add_handler(grebeshok_handler())
    app.job_queue.run_repeating(webhook_watchdog, interval=60, first=60)
    return app


ptb_app = build_ptb_app()


async def _ensure_webhook_current() -> None:
    desired = _desired_webhook_url()
    if not desired:
        return
    info = await ptb_app.bot.get_webhook_info()
    if info.url != desired:
        await ptb_app.bot.set_webhook(
            url=desired,
            allowed_updates=[],
            drop_pending_updates=False,
            secret_token=os.environ.get("WEBHOOK_SECRET") or None,
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    async with ptb_app:
        await ptb_app.start()
        await _ensure_webhook_current()
        yield
        await ptb_app.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.post("/webhook")
async def telegram_webhook(request: Request):
    secret = os.environ.get("WEBHOOK_SECRET")
    if secret:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if not hdr or not hmac.compare_digest(hdr, secret):
            return Response(status_code=HTTPStatus.UNAUTHORIZED)
    update = Update.de_json(await request.json(), ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=HTTPStatus.OK)


def _desired_webhook_url() -> Optional[str]:
    public_url = os.environ.get("PUBLIC_URL")
    if not public_url:
        return None
    return public_url.rstrip("/") + "/webhook"


@app.get("/set_webhook")
async def set_webhook():
    url = _desired_webhook_url()
    if not url:
        return {"ok": False, "error": "PUBLIC_URL не задан в окружении"}
    ok = await ptb_app.bot.set_webhook(
        url=url,
        allowed_updates=[],
        drop_pending_updates=False,
        secret_token=os.environ.get("WEBHOOK_SECRET") or None,
    )
    return {"ok": ok, "url": url}


@app.get("/reset_webhook")
async def reset_webhook():
    ok = await ptb_app.bot.delete_webhook(drop_pending_updates=True)
    return {"ok": ok}
