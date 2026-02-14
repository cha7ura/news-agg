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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
