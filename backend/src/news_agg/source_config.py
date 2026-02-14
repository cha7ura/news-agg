"""Per-source configuration loaded from sources.yaml.

Provides URL patterns, CSS selectors, and archive pagination settings
for each news source. New sources are added by editing sources.yaml.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_CONFIG: dict | None = None
_CONFIG_PATH = Path(__file__).parent / "sources.yaml"


def _load() -> dict:
    global _CONFIG
    if _CONFIG is None:
        with open(_CONFIG_PATH) as f:
            _CONFIG = yaml.safe_load(f)
    return _CONFIG


def get_source_config(slug: str) -> dict | None:
    """Get config for a source by slug. Returns None if not found."""
    return _load().get(slug)


def get_all_source_slugs() -> list[str]:
    """Get all configured source slugs."""
    return list(_load().keys())


def get_selectors(slug: str) -> dict[str, list[str]]:
    """Get CSS selectors for a source, falling back to defaults."""
    config = get_source_config(slug)
    if config and "selectors" in config:
        return config["selectors"]
    # Fallback defaults
    return {
        "title": ["h1.article-title", "article h1", "h1"],
        "content": ["article .entry-content", ".article-body", ".article-content", "article"],
        "date": ["time[datetime]", ".publish-date", ".article-date"],
        "author": [".author-name", ".byline", '[rel="author"]'],
        "image": [".article-image img", "article img"],
    }


def get_date_meta_tags(slug: str) -> list[str]:
    """Get meta tag names for date extraction."""
    config = get_source_config(slug)
    if config and "date_meta_tags" in config:
        return config["date_meta_tags"]
    return [
        "article:published_time", "og:article:published_time",
        "datePublished", "publishedTime",
    ]


def get_archive_patterns(slug: str) -> list[dict]:
    """Get archive pagination patterns for backfill.

    Returns list of dicts with 'pattern' and 'max_pages' keys.
    """
    config = get_source_config(slug)
    if not config or "sections" not in config:
        return []

    patterns = []
    for section_name, section in config["sections"].items():
        if section.get("archive_pattern"):
            patterns.append({
                "section": section_name,
                "pattern": section["archive_pattern"],
                "max_pages": section.get("max_pages", 40),
                "page_start": section.get("page_start", 1),
                "page_step": section.get("page_step", 1),
            })
    return patterns


def get_listing_urls(slug: str) -> list[str]:
    """Get listing page URLs for article discovery."""
    config = get_source_config(slug)
    if not config or "sections" not in config:
        return []

    return [
        section["listing_url"]
        for section in config["sections"].values()
        if section.get("listing_url")
    ]


def get_article_url_patterns(slug: str) -> list[str]:
    """Get regex patterns that identify valid article URLs."""
    config = get_source_config(slug)
    if config and "article_url_patterns" in config:
        return config["article_url_patterns"]
    return []


def get_skip_url_patterns(slug: str) -> list[str]:
    """Get regex patterns for URLs to skip."""
    config = get_source_config(slug)
    if config and "skip_url_patterns" in config:
        return config["skip_url_patterns"]
    return []


def get_nid_sweep_config(slug: str) -> list[dict]:
    """Get NID sweep configurations for exhaustive article discovery.

    Returns list of dicts with 'url_pattern', 'start', 'end', 'max_consecutive_404'.
    """
    config = get_source_config(slug)
    if not config or "nid_sweep" not in config:
        return []
    return config["nid_sweep"]


def get_date_sweep_config(slug: str) -> dict | None:
    """Get date sweep configuration for date-based archive discovery.

    Returns dict with 'url_pattern', 'date_format', 'start_date', or None.
    """
    config = get_source_config(slug)
    if not config or "date_sweep" not in config:
        return None
    return config["date_sweep"]
