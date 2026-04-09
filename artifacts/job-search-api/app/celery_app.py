"""Central Celery application instance for TrackNJob.

Worker:
    celery -A app.celery_app worker -Q jobs_queue --loglevel=info

Beat scheduler:
    celery -A app.celery_app beat --loglevel=info
"""

import os

from celery import Celery

from app import celery_config

celery_app = Celery(
    "tracknJob",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
    include=["app.crawler.tasks", "app.enrichment.tasks", "app.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_default_queue="jobs_queue",
    beat_schedule=celery_config.beat_schedule,
)
