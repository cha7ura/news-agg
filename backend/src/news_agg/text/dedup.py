"""Title normalization for deduplication — Sinhala-safe.

Ported from ground-news/scripts/pipeline.ts lines 173-179 (normalizeTitle).

The key insight: Sinhala script uses ZWJ (U+200D) and ZWNJ (U+200C) for conjunct
consonants. Stripping these would corrupt text like "ශ්‍රී" (Sri) into broken syllables.
"""

import re
import unicodedata


def normalize_title(title: str) -> str:
    """Normalize title for deduplication comparison.

    - NFC normalize (compose Sinhala conjuncts)
    - Lowercase
    - Keep only letters, numbers, ZWJ, ZWNJ
    - Strip everything else (punctuation, spaces, emoji)
    """
    title = unicodedata.normalize("NFC", title).lower()
    # \w matches [a-zA-Z0-9_] + Unicode letters/digits
    # We also explicitly keep ZWJ and ZWNJ for Sinhala/Tamil
    title = re.sub(r"[^\w\u200C\u200D]", "", title, flags=re.UNICODE)
    return title.strip()
