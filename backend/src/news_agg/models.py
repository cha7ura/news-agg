from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Source(BaseModel):
    id: UUID
    name: str
    slug: str
    url: str
    rss_url: str | None = None
    language: str = "en"
    is_active: bool = True


class RSSItem(BaseModel):
    title: str
    link: str
    pub_date: str | None = None
    description: str | None = None
    image_url: str | None = None


class ScrapedArticle(BaseModel):
    title: str
    content: str
    author: str | None = None
    published_at: datetime | None = None
    image_url: str | None = None
    excerpt: str | None = None
    final_url: str | None = None  # Canonical URL after redirects (for nid sweep dedup)


class ArticleCreate(BaseModel):
    source_id: UUID
    url: str
    title: str
    content: str | None = None
    excerpt: str | None = None
    image_url: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    language: str = "en"
    original_language: str = "en"


class Article(ArticleCreate):
    id: UUID
    scraped_at: datetime
    is_processed: bool = False
    created_at: datetime
    updated_at: datetime
