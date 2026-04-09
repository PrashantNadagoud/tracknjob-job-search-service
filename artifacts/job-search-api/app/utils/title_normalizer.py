"""Title normalization and seniority extraction for job listings.

``normalize_title(raw)``
    Expands common abbreviations and cleans whitespace.

``extract_seniority(title)``
    Returns one of 11 seniority levels or None.
"""

import re

# ---------------------------------------------------------------------------
# Abbreviation lookup (applied as whole-word substitutions, case-insensitive)
# ---------------------------------------------------------------------------

_ABBREV_MAP: list[tuple[str, str]] = [
    # Seniority abbreviations
    (r"\bSr\.?\b", "Senior"),
    (r"\bJr\.?\b", "Junior"),
    (r"\bAsst\.?\b", "Assistant"),
    (r"\bMgr\.?\b", "Manager"),
    (r"\bDir\.?\b", "Director"),
    (r"\bVP\b", "Vice President"),
    (r"\bEVP\b", "Executive Vice President"),
    (r"\bSVP\b", "Senior Vice President"),
    # Technical role abbreviations
    (r"\bSDE-?1\b", "Software Development Engineer I"),
    (r"\bSDE-?2\b", "Software Development Engineer II"),
    (r"\bSDE-?3\b", "Software Development Engineer III"),
    (r"\bSDE\b", "Software Development Engineer"),
    (r"\bMTS\b", "Member of Technical Staff"),
    (r"\bAMTS\b", "Associate Member of Technical Staff"),
    (r"\bPMTS\b", "Principal Member of Technical Staff"),
    (r"\bSMTS\b", "Senior Member of Technical Staff"),
    (r"\bSRE\b", "Site Reliability Engineer"),
    (r"\bDBA\b", "Database Administrator"),
    (r"\bML\b", "Machine Learning"),
    (r"\bAI\b", "Artificial Intelligence"),
    (r"\bQA\b", "Quality Assurance"),
    (r"\bQE\b", "Quality Engineer"),
    # Programme/product management
    (r"\bTPM\b", "Technical Program Manager"),
    (r"\bEM\b", "Engineering Manager"),
    (r"\bIC\b", "Individual Contributor"),
    # Generic title word abbreviations
    (r"\bEng\.?\b", "Engineer"),
    (r"\bDev\.?\b", "Developer"),
    (r"\bArch\.?\b", "Architect"),
    (r"\bSpec\.?\b", "Specialist"),
    (r"\bAdmin\.?\b", "Administrator"),
    (r"\bCoord\.?\b", "Coordinator"),
    (r"\bRep\.?\b", "Representative"),
    (r"\bOps\b", "Operations"),
    (r"\bInfra\b", "Infrastructure"),
    (r"\bSec\b(?=\s|$|-)", "Security"),
    (r"\bFull[- ]?Stack\b", "Full Stack"),
    (r"\bFE\b", "Frontend"),
    (r"\bBE\b", "Backend"),
]

# Pre-compile for performance
_COMPILED_ABBREVS: list[tuple[re.Pattern, str]] = [
    (re.compile(pattern, re.IGNORECASE), replacement)
    for pattern, replacement in _ABBREV_MAP
]

# ---------------------------------------------------------------------------
# Seniority extraction
# ---------------------------------------------------------------------------

# Order matters: more specific patterns first.
# Levels: intern, entry, associate, junior, mid, senior,
#         staff, principal, director, vp, c_level
_SENIORITY_PATTERNS: list[tuple[re.Pattern, str]] = [
    # C-level
    (re.compile(r"\b(Chief|CEO|CTO|CFO|COO|CISO|CRO|CPO|CMO)\b", re.IGNORECASE), "c_level"),
    # VP
    (re.compile(r"\b(Vice President|VP)\b", re.IGNORECASE), "vp"),
    # Director
    (re.compile(r"\b(Director|Head of|Head,)\b", re.IGNORECASE), "director"),
    # Principal
    (re.compile(r"\bPrincipal\b", re.IGNORECASE), "principal"),
    # Staff
    (re.compile(r"\bStaff\b", re.IGNORECASE), "staff"),
    # Senior / Sr
    (re.compile(r"\b(Senior|Sr\.?)\b", re.IGNORECASE), "senior"),
    # Mid-level explicit keywords
    (re.compile(r"\b(Mid[\s-]?Level|Mid[\s-]?Senior|MidLevel|III|Level\s+3)\b", re.IGNORECASE), "mid"),
    # Junior / Jr
    (re.compile(r"\b(Junior|Jr\.?)\b", re.IGNORECASE), "junior"),
    # Associate
    (re.compile(r"\bAssociate\b", re.IGNORECASE), "associate"),
    # Intern — must be BEFORE entry so "Graduate Trainee" → intern, not entry
    (re.compile(r"\b(Intern|Internship|Trainee|Apprentice|Co[\s-]?op)\b", re.IGNORECASE), "intern"),
    # Entry-level
    (re.compile(
        r"\b(Entry[\s-]?Level|Entry|New Grad|New Graduate|Graduate|Level\s+1)\b",
        re.IGNORECASE,
    ), "entry"),
]

_VALID_SENIORITY_LEVELS = frozenset(
    ["intern", "entry", "associate", "junior", "mid", "senior",
     "staff", "principal", "director", "vp", "c_level"]
)


def normalize_title(raw: str | None) -> str | None:
    """Expand abbreviations and normalize whitespace in a job title.

    Returns None for falsy input; otherwise returns the cleaned title string.
    """
    if not raw:
        return None

    title = raw.strip()
    if not title:
        return None

    for pattern, replacement in _COMPILED_ABBREVS:
        title = pattern.sub(replacement, title)

    # Collapse multiple whitespace characters
    title = re.sub(r"\s{2,}", " ", title).strip()

    return title or None


def extract_seniority(title: str | None) -> str | None:
    """Return the seniority level inferred from the job title, or None.

    Possible return values (11 levels):
        "intern", "entry", "associate", "junior", "mid", "senior",
        "staff", "principal", "director", "vp", "c_level"
    """
    if not title:
        return None

    for pattern, level in _SENIORITY_PATTERNS:
        if pattern.search(title):
            return level

    return None
