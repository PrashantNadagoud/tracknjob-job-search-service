import os

import brevo_python
from brevo_python.api import transactional_emails_api
from brevo_python.models import SendSmtpEmail, SendSmtpEmailSender, SendSmtpEmailTo


def _get_brevo_client() -> transactional_emails_api.TransactionalEmailsApi:
    configuration = brevo_python.Configuration()
    configuration.api_key["api-key"] = os.getenv("BREVO_API_KEY", "")
    return transactional_emails_api.TransactionalEmailsApi(
        brevo_python.ApiClient(configuration)
    )


def send_job_alert_email(
    to_email: str,
    search_name: str,
    new_jobs: list[dict],
) -> None:
    job_rows = ""
    for job in new_jobs:
        job_rows += f"""
        <tr>
          <td style="padding:12px;border-bottom:1px solid #f0f0f0">
            <strong>{job['title']}</strong><br>
            <span style="color:#6366f1">{job['company']}</span> · {job.get('location') or ''}<br>
            <small style="color:#9ca3af">{job.get('salary_range') or ''}</small>
          </td>
          <td style="padding:12px;border-bottom:1px solid #f0f0f0;text-align:right">
            <a href="{job['source_url']}"
               style="background:linear-gradient(to right,#6366f1,#a855f7);
                      color:white;padding:6px 14px;border-radius:20px;
                      text-decoration:none;font-size:13px">
              View Job
            </a>
          </td>
        </tr>"""

    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
      <div style="background:linear-gradient(to right,#6366f1,#a855f7,#ec4899);
                  padding:24px;border-radius:12px 12px 0 0">
        <h1 style="color:white;margin:0;font-size:22px">New Jobs for "{search_name}"</h1>
        <p style="color:rgba(255,255,255,0.8);margin:4px 0 0">
          {len(new_jobs)} new listing{'s' if len(new_jobs) > 1 else ''} found
        </p>
      </div>
      <table style="width:100%;border-collapse:collapse;background:white">
        {job_rows}
      </table>
      <div style="padding:16px;background:#f9fafb;border-radius:0 0 12px 12px;
                  text-align:center">
        <a href="{os.getenv('TNJ_FRONTEND_URL')}/job-search"
           style="color:#6366f1;font-size:13px">
          View all jobs on TrackNJob
        </a>
      </div>
    </div>"""

    client = _get_brevo_client()
    from_email = os.getenv("BREVO_FROM_EMAIL", "alerts@tracknjob.com")
    from_name = os.getenv("BREVO_FROM_NAME", "TrackNJob Alerts")
    send_smtp_email = SendSmtpEmail(
        to=[SendSmtpEmailTo(email=to_email)],
        sender=SendSmtpEmailSender(email=from_email, name=from_name),
        subject=(
            f"\U0001f514 {len(new_jobs)} new job{'s' if len(new_jobs) > 1 else ''} "
            f"matching '{search_name}'"
        ),
        html_content=html,
    )
    client.send_transac_email(send_smtp_email)
