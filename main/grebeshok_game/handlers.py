from dataclasses import dataclass
from telegram import Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

TAKE = 0

@dataclass
class GameState:
    remaining: int = 15

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data['grebeshok_state'] = GameState()
    await query.edit_message_text(
        "Игра «Гребешок» началась. Всего 15 зубцов. Возьмите от 1 до 3:")
    return TAKE

async def handle_take(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    state: GameState | None = context.user_data.get('grebeshok_state')
    if state is None:
        await update.message.reply_text('Игра не найдена, начните заново командой /start')
        return ConversationHandler.END
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text('Введите число от 1 до 3.')
        return TAKE
    take = int(text)
    if take < 1 or take > 3 or take > state.remaining:
        await update.message.reply_text('Можно взять от 1 до 3 зубцов, не больше оставшихся.')
        return TAKE
    state.remaining -= take
    if state.remaining == 0:
        await update.message.reply_text('Вы взяли последний зубец и победили!')
        return ConversationHandler.END
    bot_take = (state.remaining - 1) % 4 or 1
    bot_take = min(3, bot_take, state.remaining)
    state.remaining -= bot_take
    if state.remaining == 0:
        await update.message.reply_text(f'Я беру {bot_take} и побеждаю!')
        return ConversationHandler.END
    await update.message.reply_text(
        f'Я беру {bot_take}. Осталось {state.remaining}. Ваш ход:')
    return TAKE

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text('Игра завершена.')
    return ConversationHandler.END


def build_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CallbackQueryHandler(start, pattern='^game_grebeshok$')],
        states={
            TAKE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_take)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True,
    )
