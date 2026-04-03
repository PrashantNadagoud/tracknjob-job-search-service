"""Part 5 — Match score unit tests + search integration."""

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Listing
from app.scoring import compute_match_score, get_match_label
from tests.conftest import TEST_USER_ID


class TestComputeMatchScore:
    def test_perfect_match_no_salary(self):
        """Title(40) + Skills(45 fallback) + Remote(15) = 100."""
        prefs = {
            "desired_title": "Senior Python Engineer",
            "skills": ["Python", "FastAPI"],
            "preferred_location": None,
            "remote_only": True,
        }
        job = {
            "title": "Senior Python Engineer",
            "tags": ["Python", "FastAPI"],
            "remote": True,
            "location": None,
        }
        assert compute_match_score(job, prefs) == 100

    def test_perfect_match_with_salary(self):
        """Title(40) + Skills(35) + Remote(15) + Salary(10) = 100."""
        prefs = {
            "desired_title": "Senior Python Engineer",
            "skills": ["Python", "FastAPI"],
            "preferred_location": None,
            "remote_only": True,
            "salary_expected": True,
        }
        job = {
            "title": "Senior Python Engineer",
            "tags": ["Python", "FastAPI"],
            "remote": True,
            "location": None,
            "salary_range": "$120k-$180k",
        }
        assert compute_match_score(job, prefs) == 100

    def test_salary_fallback_boosts_skills(self):
        """Without salary data, skills weight goes from 35 to 45."""
        prefs = {
            "desired_title": None,
            "skills": ["Python", "FastAPI", "PostgreSQL", "AWS"],
            "preferred_location": None,
            "remote_only": False,
        }
        job_no_salary = {
            "title": "Any",
            "tags": ["Python", "FastAPI", "PostgreSQL", "AWS"],
            "remote": False,
            "location": None,
        }
        job_with_salary = {
            **job_no_salary,
            "salary_range": "$100k-$150k",
        }
        score_no_salary = compute_match_score(job_no_salary, prefs)
        score_with_salary = compute_match_score(job_with_salary, prefs)
        # No salary → skills get 45pts; with salary → skills get 35pts
        assert score_no_salary == 45
        assert score_with_salary == 35

    def test_no_overlap_scores_low(self):
        prefs = {
            "desired_title": "Senior Python Engineer",
            "skills": ["Python"],
            "preferred_location": None,
            "remote_only": False,
        }
        job = {
            "title": "Junior iOS Developer",
            "tags": ["Swift", "Xcode"],
            "remote": False,
            "location": "Miami",
        }
        assert compute_match_score(job, prefs) < 20

    def test_partial_skill_overlap(self):
        prefs = {
            "desired_title": None,
            "skills": ["Python", "FastAPI", "PostgreSQL", "AWS"],
            "preferred_location": None,
            "remote_only": False,
        }
        job = {
            "title": "Software Engineer",
            "tags": ["Python", "PostgreSQL", "Docker"],
            "remote": False,
            "location": None,
        }
        score = compute_match_score(job, prefs)
        # 2/3 overlap * 45 (fallback) = 30
        assert 20 <= score <= 35

    def test_score_capped_at_100(self):
        prefs = {
            "desired_title": "Engineer",
            "skills": ["Python", "FastAPI", "Go", "Rust"],
            "preferred_location": None,
            "remote_only": True,
        }
        job = {
            "title": "Engineer",
            "tags": ["Python", "FastAPI", "Go", "Rust"],
            "remote": True,
            "location": None,
        }
        assert compute_match_score(job, prefs) <= 100

    def test_empty_prefs_returns_zero(self):
        prefs = {
            "desired_title": None,
            "skills": [],
            "preferred_location": None,
            "remote_only": False,
        }
        job = {
            "title": "Some Job",
            "tags": ["Python"],
            "remote": False,
            "location": "NYC",
        }
        assert compute_match_score(job, prefs) == 0

    def test_location_partial_match_awards_points(self):
        prefs = {
            "desired_title": None,
            "skills": [],
            "preferred_location": "San Francisco",
            "remote_only": False,
        }
        job = {
            "title": "Any Job",
            "tags": [],
            "remote": False,
            "location": "San Francisco, CA",
        }
        score = compute_match_score(job, prefs)
        assert score > 0


class TestGetMatchLabel:
    def test_match_label_strong(self):
        assert get_match_label(80) == "Strong Match"
        assert get_match_label(100) == "Strong Match"

    def test_match_label_good(self):
        assert get_match_label(60) == "Good Match"
        assert get_match_label(79) == "Good Match"

    def test_match_label_partial(self):
        assert get_match_label(40) == "Partial Match"
        assert get_match_label(59) == "Partial Match"

    def test_match_label_low(self):
        assert get_match_label(0) == "Low Match"
        assert get_match_label(39) == "Low Match"

    def test_match_label_none_input_returns_none(self):
        assert get_match_label(None) is None


class TestSearchScoreIntegration:
    async def test_null_preferences_returns_null_score(
        self, async_client, db_session: AsyncSession
    ):
        """User with no preferences in DB gets match_score=null on search."""
        from tests.conftest import make_token

        fresh_uid = str(uuid.uuid4())
        token = make_token(sub=fresh_uid)
        headers = {"Authorization": f"Bearer {token}"}

        resp = await async_client.get("/api/v1/jobs/search?country=US", headers=headers)
        assert resp.status_code == 200
        body = resp.json()
        assert all(r["match_score"] is None for r in body["results"])
        assert all(r["match_label"] is None for r in body["results"])

    async def test_preferences_present_returns_scores(
        self, async_client, auth_headers, sample_preference
    ):
        """User with preferences gets integer match_score and string match_label."""
        resp = await async_client.get("/api/v1/jobs/search?country=US", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        if body["results"]:
            for result in body["results"]:
                assert isinstance(result["match_score"], int)
                assert result["match_label"] in (
                    "Strong Match", "Good Match", "Partial Match", "Low Match"
                )
