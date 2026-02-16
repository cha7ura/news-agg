from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://newsagg:newsagg@localhost:5432/newsagg"
    supabase_database_url: str = ""
    playwright_ws_url: str = "ws://localhost:3100"
    log_level: str = "info"
    rate_limit_ms: int = 500
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # SOCKS5 proxy for Playwright (e.g. "socks5://tor:9050" for Tor, or VPN proxy).
    # Uses Docker service name since browser runs in Docker. Empty string = no proxy.
    proxy_url: str = ""
    # OpenRouter LLM config for article QA review agents
    openrouter_api_key: str = ""
    openrouter_model: str = "nvidia/nemotron-3-nano-30b-a3b:free"
    # LLM base URL — change to switch provider (Ollama, LM Studio, etc.)
    llm_base_url: str = "https://openrouter.ai/api/v1"
    # Langfuse Cloud observability
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_base_url: str = "https://us.cloud.langfuse.com"
    # SearXNG for web search (agentic pipeline)
    searxng_url: str = "http://localhost:8888"
    # Meilisearch for full-text article search
    meilisearch_url: str = "http://localhost:7700"
    meilisearch_api_key: str = "newsagg-meili-dev-key"
    # Cloudflare R2 for database snapshots (S3-compatible)
    r2_endpoint_url: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_bucket_name: str = "newsagg-snapshots"
    # Neo4j for Graphiti knowledge graph
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""
    neo4j_docker_image: str = "neo4j:5.26-community"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
