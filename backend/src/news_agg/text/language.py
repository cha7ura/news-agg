"""Language detection using Unicode ranges.

Detects Sinhala vs English by checking for Sinhala Unicode block characters.
"""

import re

# Sinhala Unicode block: U+0D80 to U+0DFF
_SINHALA_RANGE = re.compile(r"[\u0D80-\u0DFF]")

# Common Sinhala words for additional confidence
_SINHALA_WORDS = {"සහ", "හා", "ඇති", "කළ", "බව", "මෙම", "ඒ", "අද", "එම", "නව"}


def detect_language(text: str) -> str:
    """Detect if text is Sinhala ('si') or English ('en').

    Uses Unicode range detection — if >10% of the first 500 chars are
    in the Sinhala block, it's Sinhala.
    """
    sample = text[:500]
    if not sample:
        return "en"

    sinhala_chars = len(_SINHALA_RANGE.findall(sample))
    ratio = sinhala_chars / len(sample)

    if ratio > 0.10:
        return "si"

    # Fallback: check for common Sinhala words
    words = set(text.split()[:50])
    if words & _SINHALA_WORDS:
        return "si"

    return "en"
