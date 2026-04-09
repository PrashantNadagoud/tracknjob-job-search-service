# Match Scoring Reference

## Overview
The match scoring logic (`app/scoring.py`) calculates a relevance score (0-100) between a job listing and a user's preferences.

## Weights
Scores are additive based on the following weights:

| Component | Weight | Max Points |
|---|---|---|
| **Title Similarity** | 40% | 40 |
| **Skills Overlap** | 35%* | 35 (or 45 if no salary) |
| **Location Fit** | 15% | 15 |
| **Salary Inclusion** | 10% | 10 |

\* *If salary data is missing from the listing, the 10 points for salary are redistributed to Skills Overlap, making it 45%.*

## Algorithm Details

### 1. Title Similarity (40 points)
- Uses `difflib.SequenceMatcher` to compare the user's `desired_title` with the job's `title`.
- `score = SequenceMatcher().ratio() * 40`.

### 2. Skills Overlap (35/45 points)
- Compares user's `skills` array with job's `tags` array (both lowercased).
- Calculated as: `(overlap / min(user_skills_count, job_tags_count, 4)) * weight`.
- Caps the comparison at 4 skills to avoid penalizing specialized roles with fewer tags.

### 3. Location / Remote Fit (15 points)
- If user wants `remote_only` and job is `remote`: **15 points**.
- Else if `preferred_location` matches job `location`:
    - Similarity > 0.6: **15 points**.
    - Similarity > 0.3: **7.5 points**.

### 4. Salary Inclusion (10 points)
- Currently binary: If the job provides a `salary_range` and the user has indicated they care about salary info, **10 points** are awarded.

## Labels
The score is mapped to a human-readable label:
- **80+**: "Strong Match"
- **60-79**: "Good Match"
- **40-59**: "Partial Match"
- **<40**: "Low Match"

## Usage in API
- Match scores are computed on-the-fly in `GET /api/v1/jobs/search` if the user has preferences set.
- Results can be sorted by `match_score`.
- Scores are NOT persisted to the database as they are user-specific and dynamic.
