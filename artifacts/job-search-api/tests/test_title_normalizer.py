"""Tests for app.utils.title_normalizer — normalize_title + extract_seniority."""

import pytest

from app.utils.title_normalizer import extract_seniority, normalize_title


# ---------------------------------------------------------------------------
# normalize_title — abbreviation expansion
# ---------------------------------------------------------------------------

class TestNormalizeTitle:
    def test_none_input_returns_none(self):
        assert normalize_title(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_title("") is None

    def test_whitespace_only_returns_none(self):
        assert normalize_title("   ") is None

    def test_sr_dot_expands_to_senior(self):
        result = normalize_title("Sr. Software Engineer")
        assert "Senior" in result
        assert "Sr." not in result

    def test_sr_no_dot_expands_to_senior(self):
        result = normalize_title("Sr Software Engineer")
        assert "Senior" in result

    def test_jr_expands_to_junior(self):
        result = normalize_title("Jr. Frontend Developer")
        assert "Junior" in result
        assert "Jr." not in result

    def test_mgr_expands_to_manager(self):
        result = normalize_title("Engineering Mgr")
        assert "Manager" in result

    def test_sde2_expands_to_full_form(self):
        result = normalize_title("SDE-2, Platform")
        assert "Software Development Engineer II" in result
        assert "SDE-2" not in result

    def test_sde3_expands_to_full_form(self):
        result = normalize_title("SDE-3")
        assert "Software Development Engineer III" in result

    def test_mts_expands_to_member_of_technical_staff(self):
        result = normalize_title("MTS, Infrastructure")
        assert "Member of Technical Staff" in result
        assert "MTS" not in result

    def test_amts_expands(self):
        result = normalize_title("AMTS - Cloud")
        assert "Associate Member of Technical Staff" in result

    def test_sre_expands_to_site_reliability_engineer(self):
        result = normalize_title("SRE, Payments")
        assert "Site Reliability Engineer" in result

    def test_tpm_expands_to_technical_program_manager(self):
        result = normalize_title("TPM, Growth")
        assert "Technical Program Manager" in result

    def test_dba_expands_to_database_administrator(self):
        result = normalize_title("DBA Lead")
        assert "Database Administrator" in result

    def test_qa_expands_to_quality_assurance(self):
        result = normalize_title("QA Engineer")
        assert "Quality Assurance" in result

    def test_sde1_expands_to_level_one(self):
        result = normalize_title("SDE1 Backend")
        assert "Software Development Engineer I" in result

    def test_vp_expands_to_vice_president(self):
        result = normalize_title("VP of Engineering")
        assert "Vice President" in result

    def test_multiple_abbreviations_in_one_title(self):
        result = normalize_title("Sr. SRE, Infra")
        assert "Senior" in result
        assert "Site Reliability Engineer" in result
        assert "Infrastructure" in result

    def test_already_expanded_title_unchanged(self):
        result = normalize_title("Senior Software Engineer")
        assert result == "Senior Software Engineer"

    def test_collapses_extra_whitespace(self):
        result = normalize_title("  Senior   Engineer  ")
        assert result == "Senior Engineer"

    def test_case_insensitive_abbreviation(self):
        result = normalize_title("sr. python developer")
        assert "Senior" in result

    def test_ml_expands_to_machine_learning(self):
        result = normalize_title("ML Engineer")
        assert "Machine Learning" in result


# ---------------------------------------------------------------------------
# extract_seniority — all 11 levels
# ---------------------------------------------------------------------------

class TestExtractSeniority:
    def test_none_input_returns_none(self):
        assert extract_seniority(None) is None

    def test_empty_string_returns_none(self):
        assert extract_seniority("") is None

    def test_unrecognized_returns_none(self):
        assert extract_seniority("Plumber") is None

    # ── 11 seniority levels ────────────────────────────────────────────────

    def test_intern(self):
        assert extract_seniority("Software Engineering Intern") == "intern"

    def test_intern_internship_keyword(self):
        assert extract_seniority("Data Science Internship") == "intern"

    def test_intern_co_op(self):
        assert extract_seniority("Co-op, Backend") == "intern"

    def test_intern_trainee(self):
        assert extract_seniority("Graduate Trainee Engineer") == "intern"

    def test_entry(self):
        assert extract_seniority("Entry Level Data Analyst") == "entry"

    def test_entry_new_grad(self):
        assert extract_seniority("New Grad Software Engineer") == "entry"

    def test_entry_graduate(self):
        assert extract_seniority("Graduate Software Engineer") == "entry"

    def test_associate(self):
        assert extract_seniority("Associate Software Engineer") == "associate"

    def test_associate_product_manager(self):
        assert extract_seniority("Associate Product Manager") == "associate"

    def test_junior(self):
        assert extract_seniority("Junior Backend Developer") == "junior"

    def test_junior_from_jr(self):
        assert extract_seniority("Jr. Python Engineer") == "junior"

    def test_mid(self):
        assert extract_seniority("Mid-Level Software Engineer") == "mid"

    def test_mid_midlevel_no_dash(self):
        assert extract_seniority("MidLevel Data Scientist") == "mid"

    def test_senior(self):
        assert extract_seniority("Senior Software Engineer") == "senior"

    def test_senior_from_sr(self):
        assert extract_seniority("Sr. Data Scientist") == "senior"

    def test_staff(self):
        assert extract_seniority("Staff Software Engineer") == "staff"

    def test_staff_engineer(self):
        assert extract_seniority("Staff Engineer, Platform") == "staff"

    def test_principal(self):
        assert extract_seniority("Principal Engineer") == "principal"

    def test_principal_scientist(self):
        assert extract_seniority("Principal Applied Scientist") == "principal"

    def test_director(self):
        assert extract_seniority("Director of Engineering") == "director"

    def test_director_head_of(self):
        assert extract_seniority("Head of Product") == "director"

    def test_vp(self):
        assert extract_seniority("Vice President of Engineering") == "vp"

    def test_vp_abbreviation(self):
        assert extract_seniority("VP, Platform") == "vp"

    def test_c_level_cto(self):
        assert extract_seniority("CTO") == "c_level"

    def test_c_level_ceo(self):
        assert extract_seniority("CEO") == "c_level"

    def test_c_level_chief(self):
        assert extract_seniority("Chief Product Officer") == "c_level"

    def test_c_level_ciso(self):
        assert extract_seniority("CISO") == "c_level"

    # ── Priority: higher level wins when multiple signals present ──────────

    def test_c_level_beats_director(self):
        assert extract_seniority("Chief Director of Engineering") == "c_level"

    def test_vp_beats_senior(self):
        assert extract_seniority("Senior VP of Engineering") == "vp"

    def test_director_beats_principal(self):
        assert extract_seniority("Principal Director of ML") == "director"

    # ── Case-insensitivity ─────────────────────────────────────────────────

    def test_case_insensitive_senior(self):
        assert extract_seniority("SENIOR software engineer") == "senior"

    def test_case_insensitive_intern(self):
        assert extract_seniority("SOFTWARE ENGINEERING INTERN") == "intern"
