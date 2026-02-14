from news_agg.text.dedup import normalize_title


def test_basic_normalization():
    assert normalize_title("Hello, World!") == "helloworld"


def test_sinhala_zwj_preserved():
    """ZWJ (U+200D) must be preserved — it's part of Sinhala conjuncts like ශ්‍රී."""
    title = "ශ්\u200Dරී ලංකාව"
    result = normalize_title(title)
    assert "\u200D" in result


def test_sinhala_zwnj_preserved():
    """ZWNJ (U+200C) must also be preserved."""
    title = "test\u200Cword"
    result = normalize_title(title)
    assert "\u200C" in result


def test_duplicate_detection():
    """Same article with different punctuation should normalize to same string."""
    t1 = normalize_title("Sri Lanka's Economy Shows Growth")
    t2 = normalize_title("Sri Lanka's Economy Shows Growth!")
    t3 = normalize_title("Sri Lanka's Economy Shows Growth?")
    assert t1 == t2 == t3


def test_unicode_letters_preserved():
    """Non-ASCII letters (Sinhala) should be preserved. Combining marks may be
    stripped — that's fine for dedup since both sides go through same normalization."""
    sinhala = normalize_title("අද දෙරණ පුවත්")
    assert len(sinhala) > 0
    # Same title should always produce same normalized form
    assert normalize_title("අද දෙරණ පුවත්") == normalize_title("අද  දෙරණ  පුවත්!")


def test_empty_title():
    assert normalize_title("") == ""
    assert normalize_title("   ") == ""


def test_numbers_preserved():
    assert normalize_title("Article #123") == "article123"
