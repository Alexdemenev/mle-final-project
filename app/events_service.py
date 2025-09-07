from fastapi import FastAPI
import requests

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """

        user_events = [] if user_id not in self.events else self.events[user_id]
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events[user_id][:k]

        return user_events

events_store = EventStore()



# создаём приложение FastAPI
app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: str):
    """
    Сохраняет событие для user_id, item_id
    
    В качестве item_id передается название счета:
    
    acc_savings,
    acc_garant,
    acc_current,
    acc_derivative,
    acc_salary,
    acc_child,
    acc_spec3,
    acc_spec1,
    acc_spec2,
    acc_short_deposit,
    acc_middle_deposit,
    acc_long_deposit,
    acc_digital,
    acc_cash,
    acc_mortgage,
    acc_pension,
    acc_credit,
    acc_tax,
    acc_credit_cart,
    acc_securities,
    acc_home,
    acc_salary_payment,
    acc_pension_loans,
    acc_debit
    """

    events_store.put(user_id, item_id)

    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """

    events = events_store.get(user_id, k)

    return {"events": events}


def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids
