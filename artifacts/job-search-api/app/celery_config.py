from celery.schedules import crontab

beat_schedule = {
    "crawl-all-companies-every-6-hours": {
        "task": "app.crawler.tasks.crawl_all_companies",
        "schedule": crontab(minute=0, hour="*/6"),
    }
}
timezone = "UTC"
