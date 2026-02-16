export interface Article {
  id: string;
  source_slug: string;
  source_name?: string;
  url: string;
  title: string | null;
  author: string | null;
  published_at: string | null;
  content: string | null;
  excerpt: string | null;
  image_url: string | null;
  language: string;
  category: string | null;
  entities: string[] | null;
  location: string | null;
  summary: string | null;
  qa_status: string | null;
  qa_score: number | null;
  created_at: string;
}

export interface Story {
  id: string;
  title: string;
  summary: string | null;
  category: string | null;
  entities: string[] | null;
  location: string | null;
  image_url: string | null;
  article_count: number;
  source_count: number;
  first_published_at: string;
  last_updated_at: string;
  sources: { name: string; slug: string }[];
}

export interface StoryDetail extends Story {
  articles: Article[];
}

export interface SearchResult {
  articles: Article[];
  count: number;
}

export interface AskResponse {
  answer: string;
  sources: { title: string; url: string; source: string; published_at?: string }[];
  session_id: string;
  articles_searched: number;
}

export interface MeiliSearchResult {
  articles: MeiliHit[];
  count: number;
  query: string;
  processing_time_ms: number;
}

export interface MeiliHit {
  id: string;
  title: string;
  excerpt: string;
  author: string | null;
  published_at: string | null;
  url: string;
  source_name: string;
  source_slug: string;
  language: string;
  category: string | null;
  qa_status: string | null;
  qa_score: number | null;
}

export interface SourceStats {
  source_slug: string;
  total_articles: number;
  reviewed: number;
  graph_saved: number;
  latest_article: string | null;
}

export interface DashboardStats {
  totals: {
    articles: number;
    reviewed: number;
    graph_saved: number;
    meilisearch_indexed: number;
    meilisearch_indexing: boolean;
  };
  sources: SourceStats[];
}
