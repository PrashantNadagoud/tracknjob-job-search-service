from celery.schedules import crontab

beat_schedule = {
    # ATS-driven pipeline — primary nightly crawl (replaces crawl_all_companies schedule)
    "run-crawl-pipeline-nightly": {
        "task": "app.crawler.tasks.run_crawl_pipeline",
        "schedule": crontab(minute=0, hour=1),
    },
    # Discovery queue — probe new company candidates every 6 hours
    "run-discovery-queue-every-6-hours": {
        "task": "app.crawler.tasks.run_discovery_queue",
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

# Note: crawl_all_companies is retained as a fallback / manually-triggered task
# but is no longer part of the beat schedule. Trigger it manually when needed:
#   celery -A app.celery_app call app.crawler.tasks.crawl_all_companies

timezone = "UTC"
