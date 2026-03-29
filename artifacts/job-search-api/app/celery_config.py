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
    "send-job-alerts-every-30-minutes": {
        "task": "app.crawler.tasks.send_job_alerts",
        "schedule": crontab(minute="*/30"),
    },
    "enrich-new-companies-nightly": {
        "task": "app.enrichment.tasks.enrich_new_companies",
        "schedule": crontab(minute=0, hour=2),
    },
    "reenrich-stale-companies-weekly": {
        "task": "app.enrichment.tasks.reenrich_stale_companies",
        "schedule": crontab(minute=0, hour=3, day_of_week="sunday"),
    },
}
timezone = "UTC"
