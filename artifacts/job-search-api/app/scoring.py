from difflib import SequenceMatcher

TITLE_WEIGHT = 40
SKILLS_WEIGHT = 35
LOCATION_WEIGHT = 15
SALARY_WEIGHT = 10


def compute_match_score(job: dict, prefs: dict) -> int:
    """
    Returns integer 0-100.
    Components:
      - Title similarity (SequenceMatcher, approximates pg_trgm): 0-40 points
      - Skills overlap: 0-35 points (0-45 when salary data is absent)
      - Location/remote match: 0-15 points
      - Salary match: 0-10 points (falls back to skills if no salary data)
    """
    score = 0
    has_salary = bool(job.get("salary_range"))

    # If no salary data, redistribute salary points to skills
    skills_weight = SKILLS_WEIGHT + SALARY_WEIGHT if not has_salary else SKILLS_WEIGHT

    # --- Title similarity ---
    if prefs.get("desired_title") and job.get("title"):
        title_sim = SequenceMatcher(
            None,
            prefs["desired_title"].lower(),
            job["title"].lower(),
        ).ratio()
        score += int(title_sim * TITLE_WEIGHT)

    # --- Skills overlap ---
    if prefs.get("skills") and job.get("tags"):
        user_skills = {s.lower() for s in prefs["skills"]}
        job_tags = {t.lower() for t in job["tags"]}
        overlap = len(user_skills & job_tags)
        max_possible = min(len(user_skills), len(job_tags), 4)
        if max_possible > 0:
            score += int((overlap / max_possible) * skills_weight)

    # --- Location / remote match ---
    if prefs.get("remote_only") and job.get("remote"):
        score += LOCATION_WEIGHT
    elif prefs.get("preferred_location") and job.get("location"):
        loc_sim = SequenceMatcher(
            None,
            prefs["preferred_location"].lower(),
            job["location"].lower(),
        ).ratio()
        if loc_sim > 0.6:
            score += LOCATION_WEIGHT
        elif loc_sim > 0.3:
            score += int(LOCATION_WEIGHT * 0.5)

    # --- Salary match (binary: job lists salary and user wants salary info) ---
    if has_salary and prefs.get("salary_expected"):
        score += SALARY_WEIGHT

    return min(score, 100)


def get_match_label(score: int | None) -> str | None:
    if score is None:
        return None
    if score >= 80:
        return "Strong Match"
    if score >= 60:
        return "Good Match"
    if score >= 40:
        return "Partial Match"
    return "Low Match"
