from difflib import SequenceMatcher


def compute_match_score(job: dict, prefs: dict) -> int:
    """
    Returns integer 0-100.
    Components:
      - Title similarity (SequenceMatcher, approximates pg_trgm): 0-60 points
      - Skills overlap: 0-25 points
      - Location/remote match: 0-15 points
    """
    score = 0

    if prefs.get("desired_title") and job.get("title"):
        title_sim = SequenceMatcher(
            None,
            prefs["desired_title"].lower(),
            job["title"].lower(),
        ).ratio()
        score += int(title_sim * 60)

    if prefs.get("skills") and job.get("tags"):
        user_skills = {s.lower() for s in prefs["skills"]}
        job_tags = {t.lower() for t in job["tags"]}
        overlap = len(user_skills & job_tags)
        max_possible = min(len(user_skills), len(job_tags), 4)
        if max_possible > 0:
            score += int((overlap / max_possible) * 25)

    if prefs.get("remote_only") and job.get("remote"):
        score += 15
    elif prefs.get("preferred_location") and job.get("location"):
        loc_sim = SequenceMatcher(
            None,
            prefs["preferred_location"].lower(),
            job["location"].lower(),
        ).ratio()
        if loc_sim > 0.6:
            score += 15
        elif loc_sim > 0.3:
            score += 8

    return min(score, 100)


def get_match_label(score: int | None) -> str | None:
    if score is None:
        return None
    if score >= 80:
        return "Strong Match"
    if score >= 60:
        return "Good Match"
    return None
