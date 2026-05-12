import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CELERY_BROKER = f"{REDIS_URL}/0"
CELERY_BACKEND = f"{REDIS_URL}/1"

# The API only needs to know how to SEND tasks, not execute them.
app = Celery(
    "platform",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)
