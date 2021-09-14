from pymongo import MongoClient
import datetime
import random

client = MongoClient()
db = client.database
collections_accrual = db.accrual
collections_payment = db.payment
result = db.result

# Функция создающая коллекция Долг и Платежи, данные полей date и month заполняются рандомно

def create_test_db():
    for i in range(10):
        day = random.randrange(1, 25)
        month = random.randrange(1, 12)
        year = random.randrange(2020, 2021)
        collections_accrual.insert_one({'id': i, 'date':datetime.date(year, month, day).isoformat(), 'month': month})
    for i in range(10):
        day = random.randrange(1, 25)
        month = random.randrange(1, 12)
        year = random.randrange(2020, 2021)
        collections_payment.insert_one({'id': i, 'date':datetime.date(year, month, day).isoformat(), 'month': month})

# Функция сопоставляющая имеющиеся долги и поступившие платежи, результатом является коллекция
# сполями id_acrrual - id долга, id_payment - id платежа

def calculated_accrual_payment():

    result = []

    payments = list(db.payment.aggregate([{'$sort': {'date': 1}}]))
    accruals = list(db.accrual.aggregate([{'$sort': {'date': 1}}]))

    for pay in payments:
        current_accruals = filter(lambda x: x['month'] == pay['month'], accruals)
        for c_accrual in current_accruals:
            # Проверяется условие, что полученные элементы по полю month имеют один год с проверяемым плаежом
            # и были созданы до появления платежа
            if (datetime.date.fromisoformat(c_accrual['date']).year == datetime.date.fromisoformat(pay['date']).year) and\
                    c_accrual['date'] <= pay['date']:
                result.append({'id_accrual': c_accrual['id'], 'id_payments': pay['id']})
                for x in range(len(accruals)):
                    if accruals[x]['id'] == c_accrual['id']:
                        del accruals[x]
                        break
            # Если платеж не смог погасить не один долг в своем месяце, ищется самый старый долг,
            # который он может погасить
            else:
                last_accrual = min(accruals, key=lambda d: d.get('date'))
                if last_accrual['date'] < pay['date']:
                    result.append({'id_accrual': last_accrual['id'], 'id_payments': pay['id']})
                    for x in range(len(accruals)):
                        if accruals[x]['id'] == last_accrual['id']:
                            del accruals[x]
                            break
                else:
                    result.append({'id_accrual': 'null', 'id_payments': pay['id']})
            break
        # Данное условие срабатывает, когда остаётся последний элемент в списке accruals
        if len(accruals) == 1:
            last_accrual = accruals[0]
            if last_accrual['date'] < pay['date']:
                result.append({'id_accrual': last_accrual['id'], 'id_payments': pay['id']})
                for x in range(len(accruals)):
                    if accruals[x]['id'] == last_accrual['id']:
                        del accruals[x]
                        break
            else:
                result.append({'id_accrual': 'null', 'id_payments': pay['id']})
        result.append({'id_accrual': 'null', 'id_payments': pay['id']})
    return db.result.insert_many(result)


create_test_db()
calculated_accrual_payment()

