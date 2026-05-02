import os
from celery import Celery
from dotenv import load_dotenv

# Load .env so we can find Redis
load_dotenv()

# Lesson 3: The Broker vs Backend
# Broker (Redis DB 0): Where the tasks are stored.
# Backend (Redis DB 1): Where the results and status are stored.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CELERY_BROKER = f"{REDIS_URL}/0"
CELERY_BACKEND = f"{REDIS_URL}/1"

app = Celery(
    "indexer",
    broker=CELERY_BROKER,
    backend=CELERY_BACKEND,
    # This tells Celery where to find our @task functions
    include=["src.pipeline.tasks"]
)

# Production-grade configuration
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # How many tasks one worker should grab at a time
    worker_prefetch_multiplier=1,
    # Kill a task if it takes more than 10 minutes
    task_time_limit=600,
)

if __name__ == "__main__":
    app.start()
