import asyncio
from aiogram import Bot, Dispatcher
from config import BOT_TOKEN
from routers import router


ALLOWED_UPDATES = ['message']

bot = Bot(token=f'{BOT_TOKEN}')

dp = Dispatcher()

dp.include_router(router)


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=ALLOWED_UPDATES)


asyncio.run(main())
