"""Motivational AI paragraph generator for job alert emails.

generate_motivational_intro(user_context) → str

Falls back to a warm static message when OPENAI_API_KEY is absent or the
API call fails. Never raises.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_STATIC_FALLBACK = (
    "Each application is a step forward, and today brings new opportunities "
    "worth exploring. Take a moment to look through what matched — one of "
    "these could be exactly what you've been looking for."
)

_PROMPT_TEMPLATE = """\
You are a warm, encouraging job search companion. Write a 2-3 sentence \
personalized morning message for {name} who has been job searching for \
{days_searching} days. Today there are {jobs_found_today} new job matches, \
including a {top_job_title} role at {top_company}.

Tone: genuine, grounded, human — NOT over-the-top or cheesy.
Do not use phrases like "you've got this!" or "crush it today".
Keep it under 60 words. No subject line, just the paragraph.\
"""


def generate_motivational_intro(user_context: dict[str, Any]) -> str:
    """Return a personalised motivational paragraph for the alert email.

    Args:
        user_context: dict with keys:
            name, days_searching, jobs_found_today, top_job_title, top_company

    Returns:
        A 2-3 sentence paragraph string. Falls back to static message on any error.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        logger.debug("OPENAI_API_KEY not set; returning static motivational message")
        return _STATIC_FALLBACK

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        prompt = _PROMPT_TEMPLATE.format(
            name=user_context.get("name", "there"),
            days_searching=user_context.get("days_searching", 1),
            jobs_found_today=user_context.get("jobs_found_today", 1),
            top_job_title=user_context.get("top_job_title", "Software Engineer"),
            top_company=user_context.get("top_company", "a great company"),
        )
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.7,
        )
        text = response.choices[0].message.content or ""
        text = text.strip()
        if text:
            return text
        return _STATIC_FALLBACK
    except Exception as exc:
        logger.warning("Failed to generate motivational intro: %s", exc)
        return _STATIC_FALLBACK
