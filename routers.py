import json
from aiogram import Router, types
from utils import aggregate_salaries


router = Router()


@router.message()
async def aggregate_cmd(message: types.Message):
    try:
        params = json.loads(message.text)
        if not isinstance(params, dict):
            await message.answer("Допустимо отправлять только следующие запросы:\n"
                                 "{\"dt_from\": \"2022-09-01T00:00:00\", \"dt_upto\": \"2022-12-31T23:59:00\", \"group_type\": \"month\"}\n"
                                 "{\"dt_from\": \"2022-10-01T00:00:00\", \"dt_upto\": \"2022-11-30T23:59:00\", \"group_type\": \"day\"}\n"
                                 "{\"dt_from\": \"2022-02-01T00:00:00\", \"dt_upto\": \"2022-02-02T00:00:00\", \"group_type\": \"hour\"}")
            return
        required_fields = ["dt_from", "dt_upto", "group_type"]
        if not all(field in params for field in required_fields):
            await message.answer("Невалидный запрос. Пример запроса:\n"
                                 "{\"dt_from\": \"2022-09-01T00:00:00\", \"dt_upto\": \"2022-12-31T23:59:00\", \"group_type\": \"month\"}")
            return
        result = await aggregate_salaries(params)
        await message.answer(json.dumps(result))
    except json.JSONDecodeError:
        await message.answer("Невалидный запрос. Пример запроса:\n"
                             "{\"dt_from\": \"2022-09-01T00:00:00\", \"dt_upto\": \"2022-12-31T23:59:00\", \"group_type\": \"month\"}")
