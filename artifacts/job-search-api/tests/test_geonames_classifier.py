"""Unit tests for the GeoNames-based city lookup classifier (TRA-362).

Tests cover:
- load_geonames_index builds the in-memory dict correctly
- classify_by_geonames resolves cities to the right market
- _country_to_market maps all expected country codes
- _tokenize_location handles common location string formats
- classify_listing uses GeoNames as step 2 in the pipeline
- Cities not in any known market return None (fall-through)
- Index not loaded → graceful None fallback
"""

import pytest

from app.crawler.geo_classifier import (
    _EU_COUNTRY_CODES,
    _country_to_market,
    _tokenize_location,
    classify_by_geonames,
    classify_listing,
    load_geonames_index,
)
import app.crawler.geo_classifier as _gc_module


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_sample_index():
    """Load a representative subset of the GeoNames dataset for testing.

    Rows are ordered by population DESC (highest first) so that the most
    populous city wins when the same name exists in multiple countries —
    matching the ORDER BY population DESC used in the real startup query.
    """
    rows = [
        # India (high population → listed first so they win on duplicate names)
        ("Mumbai",         "Mumbai",           "IN"),
        ("New Delhi",      "New Delhi",        "IN"),
        ("Bengaluru",      "Bengaluru",        "IN"),
        ("Bangalore",      "Bangalore",        "IN"),
        ("Hyderabad",      "Hyderabad",        "IN"),
        ("Chennai",        "Chennai",          "IN"),
        ("Pune",           "Pune",             "IN"),
        ("Gurugram",       "Gurugram",         "IN"),
        ("Gurgaon",        "Gurgaon",          "IN"),
        # US
        ("New York City",  "New York City",    "US"),
        ("Chicago",        "Chicago",          "US"),
        ("San Francisco",  "San Francisco",    "US"),
        ("Seattle",        "Seattle",          "US"),
        ("Austin",         "Austin",           "US"),
        # EU — London GB listed before smaller London CA
        ("London",         "London",           "GB"),
        ("Berlin",         "Berlin",           "DE"),
        ("Paris",          "Paris",            "FR"),
        ("Amsterdam",      "Amsterdam",        "NL"),
        ("Dublin",         "Dublin",           "IE"),
        ("Warsaw",         "Warsaw",           "PL"),
        ("Stockholm",      "Stockholm",        "SE"),
        ("Zurich",         "Zurich",           "CH"),
        # Other markets (should return None from classify_by_geonames)
        ("Seoul",          "Seoul",            "KR"),
        ("Tokyo",          "Tokyo",            "JP"),
        ("Kuala Lumpur",   "Kuala Lumpur",     "MY"),
        ("Singapore",      "Singapore",        "SG"),
        ("Ho Chi Minh City", "Ho Chi Minh City", "VN"),
        ("Tel Aviv",       "Tel Aviv",         "IL"),
        ("Makati",         "Makati",           "PH"),
    ]
    load_geonames_index(rows)


# ---------------------------------------------------------------------------
# load_geonames_index
# ---------------------------------------------------------------------------

class TestLoadGeonamesIndex:
    def test_index_populated(self):
        _load_sample_index()
        assert _gc_module._geonames_index is not None
        assert len(_gc_module._geonames_index) > 0

    def test_keys_are_lowercase(self):
        _load_sample_index()
        index = _gc_module._geonames_index
        for key in index:
            assert key == key.lower(), f"Key {key!r} is not lowercase"

    def test_ascii_name_also_indexed(self):
        load_geonames_index([("Zürich", "Zurich", "CH")])
        index = _gc_module._geonames_index
        assert "zürich" in index
        assert "zurich" in index

    def test_empty_name_skipped(self):
        load_geonames_index([("", "ascii", "US"), ("Valid City", "Valid City", "US")])
        index = _gc_module._geonames_index
        assert "" not in index
        assert "valid city" in index

    def test_first_country_wins_on_duplicate_city(self):
        load_geonames_index([
            ("Springfield", "Springfield", "US"),
            ("Springfield", "Springfield", "GB"),
        ])
        index = _gc_module._geonames_index
        assert index["springfield"] == "US"


# ---------------------------------------------------------------------------
# _country_to_market
# ---------------------------------------------------------------------------

class TestCountryToMarket:
    def test_us(self):
        assert _country_to_market("US") == "US"

    def test_india(self):
        assert _country_to_market("IN") == "IN"

    @pytest.mark.parametrize("cc", ["DE", "FR", "GB", "NL", "IE", "PL", "SE", "CH", "NO", "IS"])
    def test_eu_codes(self, cc):
        assert _country_to_market(cc) == "EU"

    @pytest.mark.parametrize("cc", ["PH", "VN", "KR", "MY", "IL", "SG", "JP", "BR", "AU"])
    def test_other_market_returns_none(self, cc):
        assert _country_to_market(cc) is None

    def test_case_insensitive(self):
        assert _country_to_market("de") == "EU"
        assert _country_to_market("us") == "US"
        assert _country_to_market("in") == "IN"


# ---------------------------------------------------------------------------
# _tokenize_location
# ---------------------------------------------------------------------------

class TestTokenizeLocation:
    def test_simple_city(self):
        tokens = _tokenize_location("Berlin")
        assert "berlin" in tokens

    def test_city_country(self):
        tokens = _tokenize_location("Berlin, Germany")
        assert "berlin" in tokens
        assert "germany" in tokens

    def test_city_state(self):
        tokens = _tokenize_location("San Francisco, CA")
        assert "san francisco" in tokens
        assert "ca" in tokens

    def test_multi_word_city(self):
        tokens = _tokenize_location("Ho Chi Minh City, VN")
        assert "ho chi minh city" in tokens

    def test_empty_string(self):
        tokens = _tokenize_location("")
        assert tokens == []

    def test_pipe_separator(self):
        tokens = _tokenize_location("London | UK")
        assert "london" in tokens
        assert "uk" in tokens

    def test_slash_separator(self):
        tokens = _tokenize_location("Berlin/Germany")
        assert "berlin" in tokens


# ---------------------------------------------------------------------------
# classify_by_geonames
# ---------------------------------------------------------------------------

class TestClassifyByGeonames:
    def setup_method(self):
        _load_sample_index()

    def test_gurugram_returns_in(self):
        assert classify_by_geonames("Gurugram") == "IN"

    def test_gurugram_with_country_suffix(self):
        assert classify_by_geonames("Gurugram, India") == "IN"

    def test_bangalore_returns_in(self):
        assert classify_by_geonames("Bangalore") == "IN"

    def test_bengaluru_returns_in(self):
        assert classify_by_geonames("Bengaluru, Karnataka") == "IN"

    def test_mumbai_returns_in(self):
        assert classify_by_geonames("Mumbai") == "IN"

    def test_new_york_returns_us(self):
        assert classify_by_geonames("New York City") == "US"

    def test_san_francisco_returns_us(self):
        assert classify_by_geonames("San Francisco, CA") == "US"

    def test_seattle_returns_us(self):
        assert classify_by_geonames("Seattle") == "US"

    def test_berlin_returns_eu(self):
        assert classify_by_geonames("Berlin") == "EU"

    def test_berlin_with_country(self):
        assert classify_by_geonames("Berlin, Germany") == "EU"

    def test_london_returns_eu(self):
        assert classify_by_geonames("London") == "EU"

    def test_amsterdam_returns_eu(self):
        assert classify_by_geonames("Amsterdam") == "EU"

    def test_makati_returns_none(self):
        """Makati (Philippines) → PH → OTHER market → None"""
        assert classify_by_geonames("Makati") is None

    def test_ho_chi_minh_city_returns_none(self):
        """Ho Chi Minh City (Vietnam) → VN → OTHER market → None"""
        assert classify_by_geonames("Ho Chi Minh City") is None

    def test_seoul_returns_none(self):
        assert classify_by_geonames("Seoul") is None

    def test_kuala_lumpur_returns_none(self):
        assert classify_by_geonames("Kuala Lumpur") is None

    def test_tel_aviv_returns_none(self):
        assert classify_by_geonames("Tel Aviv") is None

    def test_unknown_city_returns_none(self):
        assert classify_by_geonames("Nonexistent Xyztown") is None

    def test_empty_location_returns_none(self):
        assert classify_by_geonames("") is None

    def test_remote_string_returns_none(self):
        assert classify_by_geonames("Remote") is None

    def test_index_not_loaded_returns_none(self):
        original = _gc_module._geonames_index
        _gc_module._geonames_index = None
        try:
            result = classify_by_geonames("Berlin")
            assert result is None
        finally:
            _gc_module._geonames_index = original


# ---------------------------------------------------------------------------
# classify_listing with GeoNames as step 2
# ---------------------------------------------------------------------------

class TestClassifyListingWithGeonames:
    def setup_method(self):
        _load_sample_index()

    def test_geonames_step2_gurugram_no_country_hint(self):
        """No country code provided; GeoNames step 2 resolves Gurugram → IN."""
        result = classify_listing(
            location_raw="Gurugram",
            description="",
            work_type="",
            country=None,
        )
        assert result == "IN"

    def test_geonames_step2_berlin_no_country_hint(self):
        """GeoNames step 2 resolves Berlin → EU even without country hint."""
        result = classify_listing(
            location_raw="Berlin",
            description="",
            work_type="",
            country=None,
        )
        assert result == "EU"

    def test_geonames_step2_san_francisco_no_country_hint(self):
        result = classify_listing(
            location_raw="San Francisco, CA",
            description="",
            work_type="",
            country=None,
        )
        assert result == "US"

    def test_step1_country_code_beats_geonames(self):
        """Explicit country code (step 1) overrides GeoNames result."""
        result = classify_listing(
            location_raw="Berlin",
            description="",
            work_type="",
            country="IN",
        )
        assert result == "IN"

    def test_makati_falls_through_to_other(self):
        """Makati is PH; GeoNames returns None; no signals; non-empty loc → OTHER."""
        result = classify_listing(
            location_raw="Makati",
            description="",
            work_type="",
            country=None,
        )
        assert result == "OTHER"

    def test_malaysia_not_in_us_feed(self):
        result = classify_listing(
            location_raw="Kuala Lumpur",
            description="",
            work_type="",
            country=None,
        )
        assert result == "OTHER"

    def test_israel_not_in_us_feed(self):
        result = classify_listing(
            location_raw="Tel Aviv",
            description="",
            work_type="",
            country=None,
        )
        assert result == "OTHER"

    def test_korea_not_in_us_feed(self):
        result = classify_listing(
            location_raw="Seoul",
            description="",
            work_type="",
            country=None,
        )
        assert result == "OTHER"

    def test_geonames_step2_skipped_when_index_empty(self):
        """When GeoNames index is not loaded, pipeline falls through to signals."""
        original = _gc_module._geonames_index
        _gc_module._geonames_index = None
        try:
            result = classify_listing(
                location_raw="Bangalore, India",
                description="",
                work_type="",
                country=None,
            )
            assert result == "IN"
        finally:
            _gc_module._geonames_index = original
