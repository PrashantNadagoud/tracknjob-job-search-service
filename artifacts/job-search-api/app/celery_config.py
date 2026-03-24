from celery.schedules import crontab

beat_schedule = {
    "crawl-all-companies-every-6-hours": {
        "task": "app.crawler.tasks.crawl_all_companies",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "deactivate-stale-jobs-every-12-hours": {
        "task": "app.crawler.tasks.deactivate_stale_jobs",
        "schedule": crontab(minute=0, hour="*/12"),
    },
}
timezone = "UTC"
