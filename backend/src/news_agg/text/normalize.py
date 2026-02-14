"""Text normalization with Sinhala/Tamil Unicode support.

Ported from ground-news/scripts/pipeline.ts lines 142-170 (normalizeText).
"""

import html
import re
import unicodedata

# Double-encoded UTF-8 patterns (mojibake) — shows as Ã¢â‚¬ etc.
_MOJIBAKE_REPLACEMENTS = {
    "Ã¢â\u201aÂ¬â\u201e¢": "\u2019",  # right single quote
    "Ã¢â\u201aÂ¬â\u20ac\u201d": "\u2014",  # em dash
    "Ã¢â\u201aÂ¬Â¦": "\u2026",  # ellipsis
}


def normalize_text(text: str) -> str:
    """Normalize Unicode text — fix common encoding issues in Sinhala/Tamil content.

    Steps:
    1. NFC normalization (critical for Sinhala conjunct consonants)
    2. HTML entity decoding (&amp; → &, &#8217; → ', etc.)
    3. Mojibake fix (double-encoded UTF-8)
    4. Collapse whitespace
    """
    # NFC for composed form — important for Sinhala conjuncts
    text = unicodedata.normalize("NFC", text)

    # Decode HTML entities (handles both named and numeric)
    text = html.unescape(text)

    # Fix double-encoded UTF-8
    for bad, good in _MOJIBAKE_REPLACEMENTS.items():
        text = text.replace(bad, good)

    # Collapse multiple whitespace to single space
    text = re.sub(r"\s+", " ", text).strip()

    return text
