from celery.schedules import crontab

beat_schedule = {
    # ATS-driven pipeline — runs every 6 hours
    "run-crawl-pipeline-every-6-hours": {
        "task": "app.crawler.tasks.run_crawl_pipeline",
        "schedule": crontab(minute=0, hour="*/6"),
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
    "reactivate-errored-sources-every-6-hours": {
        "task": "app.crawler.tasks.reactivate_errored_sources",
        "schedule": crontab(minute=30, hour="*/6"),
    },
    "enrich-new-companies-nightly": {
        "task": "app.enrichment.tasks.enrich_new_companies",
        "schedule": crontab(minute=0, hour=2),
    },
    "reenrich-stale-companies-weekly": {
        "task": "app.enrichment.tasks.reenrich_stale_companies",
        "schedule": crontab(minute=0, hour=3, day_of_week="sunday"),
    },
    # Job alerts — runs every hour, self-filters per delivery_time_utc per subscription
    "send-daily-alerts": {
        "task": "app.alert_tasks.send_daily_alerts",
        "schedule": crontab(minute=0),
    },
    # Retry — re-attempt today's failed deliveries with exponential backoff
    # Runs every 5 minutes so it can honour the short first-retry window (5 min).
    "retry-failed-deliveries-every-5-minutes": {
        "task": "app.alert_tasks.retry_failed_deliveries",
        "schedule": crontab(minute="*/5"),
    },
    # Retention — delete alert_deliveries rows older than 90 days
    "prune-old-deliveries-nightly": {
        "task": "app.alert_tasks.prune_old_deliveries",
        "schedule": crontab(minute=30, hour=4),
    },
    # YC discovery — fetch full YC company list weekly and queue new companies for probing
    "seed-yc-discovery-weekly": {
        "task": "app.crawler.tasks.seed_yc_discovery",
        "schedule": crontab(minute=0, hour=1, day_of_week="sunday"),
    },
}

# Note: crawl_all_companies is retained as a fallback / manually-triggered task
# but is no longer part of the beat schedule. Trigger it manually when needed:
#   celery -A app.celery_app call app.crawler.tasks.crawl_all_companies

timezone = "UTC"
