from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from motor.motor_asyncio import AsyncIOMotorClient
from bson.codec_options import CodecOptions
import pytz


async def get_db_collection():
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.test_db
    collection = db.get_collection('salaries',codec_options=CodecOptions(tz_aware=True,tzinfo=pytz.utc))
    return collection


async def aggregate_salaries(params):
    collection = await get_db_collection()

    # Преобразуем даты из строки в объект datetime
    dt_from = datetime.fromisoformat(params["dt_from"])
    dt_upto = datetime.fromisoformat(params["dt_upto"])
    group_type = params["group_type"]

    # Создаем словарь для добавления полей в документы
    add_fields = {
        "year": {"$year": "$dt"},
        "month": {"$month": "$dt"},
    }
    # Добавляем поле 'day', если группируем по дням
    if group_type == "day":
        add_fields["day"] = {"$dayOfMonth": "$dt"}
    # Добавляем поля 'day' и 'hour', если группируем по часам
    elif group_type == "hour":
        add_fields["day"] = {"$dayOfMonth": "$dt"}
        add_fields["hour"] = {"$hour": "$dt"}

    # Создаем конвейер агрегации
    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": dt_from,
                    "$lte": dt_upto
                }
            }
        },
        {
            "$addFields": add_fields
        },
        {
            "$group": {
                "_id": add_fields,
                "totalSalary": {
                    "$sum": "$value"
                }
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]

    # Выполняем агрегацию
    cursor = collection.aggregate(pipeline)

    # Создаем список всех дат в заданном диапазоне
    start_date = datetime.fromisoformat(params["dt_from"])
    end_date = datetime.fromisoformat(params["dt_upto"])

    if group_type == "month":
        all_dates = [start_date + relativedelta(months=+x) for x in
                     range((end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1)]
    elif group_type == "day":
        all_dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    elif group_type == "hour":
        all_dates = [start_date + timedelta(hours=x) for x in
                     range(int((end_date - start_date).total_seconds() // 3600) + 1)]

    # Преобразуем все даты в формат ISO
    all_dates = [date.isoformat() for date in all_dates]
    remaining_dates = all_dates.copy()

    dataset = []
    labels = []

    # Обходим все документы в курсоре
    async for document in cursor:
        if group_type == "month":
            date_str = datetime(document['_id']['year'], document['_id']['month'], 1).isoformat()
        elif group_type == "day":
            date_str = datetime(document['_id']['year'], document['_id']['month'], document['_id']['day']).isoformat()
        elif group_type == "hour":
            date_str = datetime(document['_id']['year'], document['_id']['month'], document['_id']['day'],
                                document['_id']['hour']).isoformat()

        # Если дата есть в списке оставшихся дат, добавляем зарплату и дату в списки
        if date_str in remaining_dates:
            dataset.append(document['totalSalary'])
            labels.append(date_str)
            remaining_dates.remove(date_str)

    # Добавляем 0 для дат, которые отсутствуют в курсоре
    for date in remaining_dates:
        index = all_dates.index(date)
        dataset.insert(index, 0)
        labels.insert(index, date)

    return {"dataset": dataset, "labels": labels}
