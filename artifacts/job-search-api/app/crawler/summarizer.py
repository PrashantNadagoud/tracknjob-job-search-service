import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a helpful assistant that returns valid JSON only. "
    "Do not include markdown, code fences, or any text outside the JSON object."
)

_USER_PROMPT_TEMPLATE = """Given this job listing:
Title: {title}
Company: {company}
Location: {location}

Return a JSON object with exactly these fields:
- summary: a 2-sentence plain English summary of the role
- tags: a list of up to 6 technology or skill keywords
- salary_range: extracted salary range as a string, or null if not mentioned"""

_EMPTY_RESULT: dict[str, Any] = {
    "summary": None,
    "tags": None,
    "salary_range": None,
}


async def generate_summary(
    title: str,
    company: str,
    location: str | None,
) -> dict[str, Any]:
    """Call OpenAI gpt-4o-mini to produce summary, tags, and salary_range.

    Returns a dict with those three keys.  On any failure the dict values are
    all None so the caller never needs to handle exceptions.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; skipping summarization")
        return _EMPTY_RESULT

    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=api_key)
        prompt = _USER_PROMPT_TEMPLATE.format(
            title=title,
            company=company,
            location=location or "Not specified",
        )
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        raw = response.choices[0].message.content or "{}"
        result = json.loads(raw)
        return {
            "summary": result.get("summary") or None,
            "tags": result.get("tags") or None,
            "salary_range": result.get("salary_range") or None,
        }
    except Exception:
        logger.exception(
            "OpenAI summarization failed for '%s' @ %s; leaving fields null",
            title,
            company,
        )
        return _EMPTY_RESULT
