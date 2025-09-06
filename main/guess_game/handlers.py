from dataclasses import dataclass
import random
from telegram import Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

ASK = 0

@dataclass
class GuessState:
    target: int = random.randint(1, 100)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data['guess_state'] = GuessState()
    await query.edit_message_text(
        "Игра «Угадай число»! Я загадал число от 1 до 100. Попробуйте угадать:")
    return ASK

async def handle_guess(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    state: GuessState | None = context.user_data.get('guess_state')
    if state is None:
        await update.message.reply_text('Игра не найдена, начните заново командой /start')
        return ConversationHandler.END
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text('Введите число.')
        return ASK
    guess = int(text)
    if guess < state.target:
        await update.message.reply_text('Больше.')
        return ASK
    if guess > state.target:
        await update.message.reply_text('Меньше.')
        return ASK
    await update.message.reply_text('Верно! Спасибо за игру.')
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text('Игра остановлена.')
    return ConversationHandler.END


def build_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(start, pattern='^game_guess$')],
        states={
            ASK: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_guess)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True,
    )
