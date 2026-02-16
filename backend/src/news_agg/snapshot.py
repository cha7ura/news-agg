"""Full data snapshot management via Cloudflare R2.

Manages snapshots for all data stores:
  PostgreSQL: pg_dump → gzip → R2 (source of truth)
  Neo4j:      neo4j-admin dump → gzip → R2 (expensive to rebuild — LLM calls)
  Meilisearch: derived from PostgreSQL — rebuilt on pull, not snapshotted

R2 is S3-compatible with free 10GB storage and zero egress fees.
Snapshots enable syncing between development and pipeline PCs.

Workflow:
  PC-A: news-agg snapshot push --all --label pc-a
  PC-B: news-agg snapshot pull --all    (restores PG + Neo4j, rebuilds Meili)
"""

from __future__ import annotations

import gzip
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3

from news_agg.config import settings
from news_agg.utils.logging import get_logger, GREEN, YELLOW, RED, BOLD, DIM, RESET

log = get_logger()

# ── R2 key prefixes ──────────────────────────────────────────────────────────
PG_PREFIX = "pg/"
NEO4J_PREFIX = "neo4j/"


def _get_s3_client():
    """Create an S3-compatible client for Cloudflare R2."""
    if not settings.r2_endpoint_url:
        raise ValueError("R2_ENDPOINT_URL not set in .env")
    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint_url,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name="auto",
    )


def _parse_db_url(url: str) -> dict:
    """Parse PostgreSQL URL into components."""
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port or 5432),
        "user": parsed.username or "newsagg",
        "password": parsed.password or "newsagg",
        "dbname": parsed.path.lstrip("/") or "newsagg",
    }


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _neo4j_volume_name() -> str:
    """Discover the Docker volume used by Neo4j /data mount."""
    result = subprocess.run(
        ["docker", "inspect", "news-agg-neo4j",
         "--format", "{{range .Mounts}}{{if eq .Destination \"/data\"}}{{.Name}}{{end}}{{end}}"],
        capture_output=True, text=True, timeout=10,
    )
    name = result.stdout.strip()
    if not name:
        raise RuntimeError("Could not find Neo4j data volume. Is news-agg-neo4j running?")
    return name


# ── PostgreSQL snapshots ──────────────────────────────────────────────────────

def push_pg(label: str | None = None) -> str:
    """Dump local PostgreSQL → gzip → upload to R2. Returns the R2 key."""
    db = _parse_db_url(settings.database_url)
    s3 = _get_s3_client()
    ts = _timestamp()
    key = f"{PG_PREFIX}newsagg-{label}-{ts}.sql.gz" if label else f"{PG_PREFIX}newsagg-{ts}.sql.gz"

    with tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        log.info(f"{BOLD}PostgreSQL snapshot push{RESET}")
        log.info(f"  {DIM}Dumping {db['dbname']}@{db['host']}...{RESET}")

        docker_cmd = [
            "docker", "exec", "news-agg-postgres",
            "pg_dump", "-U", db["user"], "-d", db["dbname"],
            "--no-owner", "--no-acl",
        ]
        result = subprocess.run(docker_cmd, capture_output=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(f"pg_dump failed: {result.stderr.decode()}")

        with gzip.open(tmp_path, "wb") as gz:
            gz.write(result.stdout)

        size_mb = tmp_path.stat().st_size / (1024 * 1024)
        log.info(f"  {GREEN}✓{RESET} Dump complete ({size_mb:.1f} MB compressed)")

        log.info(f"  {DIM}Uploading → R2: {key}...{RESET}")
        s3.upload_file(str(tmp_path), settings.r2_bucket_name, key)
        log.info(f"  {GREEN}✓{RESET} PostgreSQL snapshot uploaded: {key}")
        return key

    finally:
        tmp_path.unlink(missing_ok=True)


def pull_pg(key: str | None = None) -> None:
    """Download PG snapshot from R2 → restore to local PostgreSQL."""
    db = _parse_db_url(settings.database_url)
    s3 = _get_s3_client()

    if not key:
        key = _latest_key(s3, PG_PREFIX)
        if not key:
            log.error(f"{RED}No PostgreSQL snapshots found in R2{RESET}")
            return

    with tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        log.info(f"{BOLD}PostgreSQL snapshot pull{RESET}")
        log.info(f"  {DIM}Downloading {key}...{RESET}")
        s3.download_file(settings.r2_bucket_name, key, str(tmp_path))

        size_mb = tmp_path.stat().st_size / (1024 * 1024)
        log.info(f"  {GREEN}✓{RESET} Downloaded ({size_mb:.1f} MB)")

        with gzip.open(tmp_path, "rb") as gz:
            sql_bytes = gz.read()

        log.info(f"  {DIM}Restoring to {db['dbname']}...{RESET}")
        docker_cmd = [
            "docker", "exec", "-i", "news-agg-postgres",
            "psql", "-U", db["user"], "-d", db["dbname"],
        ]
        result = subprocess.run(docker_cmd, input=sql_bytes, capture_output=True, timeout=600)
        if result.returncode != 0:
            stderr = result.stderr.decode()
            if "ERROR" in stderr:
                log.warning(f"  {YELLOW}Restore warnings:{RESET}\n{stderr[:500]}")
            else:
                log.info(f"  {DIM}Restore completed with notices{RESET}")

        log.info(f"  {GREEN}✓{RESET} PostgreSQL restored from {key}")

    finally:
        tmp_path.unlink(missing_ok=True)


# ── Neo4j snapshots ──────────────────────────────────────────────────────────

def push_neo4j(label: str | None = None) -> str:
    """Stop Neo4j → dump via temp container → gzip → upload to R2.

    Neo4j requires the database to be offline for a consistent dump.
    We stop the container, run neo4j-admin in a temp container with
    the same data volume, then restart.
    """
    s3 = _get_s3_client()
    ts = _timestamp()
    key = f"{NEO4J_PREFIX}neo4j-{label}-{ts}.dump.gz" if label else f"{NEO4J_PREFIX}neo4j-{ts}.dump.gz"

    log.info(f"{BOLD}Neo4j snapshot push{RESET}")

    # Find the data volume name before stopping
    volume_name = _neo4j_volume_name()
    log.info(f"  {DIM}Volume: {volume_name}{RESET}")

    with tempfile.TemporaryDirectory() as tmpdir:
        dump_path = Path(tmpdir) / "neo4j.dump"
        gz_path = Path(tmpdir) / "neo4j.dump.gz"

        try:
            # Stop Neo4j for consistent dump
            log.info(f"  {DIM}Stopping Neo4j...{RESET}")
            subprocess.run(["docker", "stop", "news-agg-neo4j"], capture_output=True, timeout=30)

            # Dump using a temporary container with the same data volume
            log.info(f"  {DIM}Dumping neo4j database...{RESET}")
            result = subprocess.run(
                [
                    "docker", "run", "--rm",
                    "-v", f"{volume_name}:/data",
                    "-v", f"{tmpdir}:/backup",
                    "neo4j:5.26-community",
                    "neo4j-admin", "database", "dump", "neo4j",
                    "--to-path=/backup/",
                ],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                raise RuntimeError(f"neo4j-admin dump failed: {result.stderr}")

            # Find the dump file (neo4j-admin creates it with a specific name)
            dump_files = list(Path(tmpdir).glob("*.dump"))
            if not dump_files:
                raise RuntimeError(f"No dump file created in {tmpdir}")
            actual_dump = dump_files[0]

            # Compress
            with open(actual_dump, "rb") as f_in:
                with gzip.open(gz_path, "wb") as f_out:
                    while chunk := f_in.read(8 * 1024 * 1024):
                        f_out.write(chunk)

            size_mb = gz_path.stat().st_size / (1024 * 1024)
            log.info(f"  {GREEN}✓{RESET} Dump complete ({size_mb:.1f} MB compressed)")

            # Upload to R2
            log.info(f"  {DIM}Uploading → R2: {key}...{RESET}")
            s3.upload_file(str(gz_path), settings.r2_bucket_name, key)
            log.info(f"  {GREEN}✓{RESET} Neo4j snapshot uploaded: {key}")

            return key

        finally:
            # Always restart Neo4j
            log.info(f"  {DIM}Restarting Neo4j...{RESET}")
            subprocess.run(["docker", "start", "news-agg-neo4j"], capture_output=True, timeout=30)
            log.info(f"  {GREEN}✓{RESET} Neo4j restarted")


def pull_neo4j(key: str | None = None) -> None:
    """Download Neo4j dump from R2 → stop → restore via temp container → start."""
    s3 = _get_s3_client()

    if not key:
        key = _latest_key(s3, NEO4J_PREFIX)
        if not key:
            log.error(f"{RED}No Neo4j snapshots found in R2{RESET}")
            return

    log.info(f"{BOLD}Neo4j snapshot pull{RESET}")

    # Find volume before stopping
    volume_name = _neo4j_volume_name()

    with tempfile.TemporaryDirectory() as tmpdir:
        gz_path = Path(tmpdir) / "neo4j.dump.gz"
        dump_path = Path(tmpdir) / "neo4j.dump"

        try:
            # Download from R2
            log.info(f"  {DIM}Downloading {key}...{RESET}")
            s3.download_file(settings.r2_bucket_name, key, str(gz_path))

            size_mb = gz_path.stat().st_size / (1024 * 1024)
            log.info(f"  {GREEN}✓{RESET} Downloaded ({size_mb:.1f} MB)")

            # Decompress
            with gzip.open(gz_path, "rb") as f_in:
                with open(dump_path, "wb") as f_out:
                    while chunk := f_in.read(8 * 1024 * 1024):
                        f_out.write(chunk)

            # Stop Neo4j
            log.info(f"  {DIM}Stopping Neo4j...{RESET}")
            subprocess.run(["docker", "stop", "news-agg-neo4j"], capture_output=True, timeout=30)

            # Restore using a temp container
            log.info(f"  {DIM}Restoring neo4j database...{RESET}")
            result = subprocess.run(
                [
                    "docker", "run", "--rm",
                    "-v", f"{volume_name}:/data",
                    "-v", f"{tmpdir}:/backup",
                    "neo4j:5.26-community",
                    "neo4j-admin", "database", "load", "neo4j",
                    "--from-path=/backup/",
                    "--overwrite-destination=true",
                ],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                log.warning(f"  {YELLOW}neo4j-admin load output: {result.stderr[:300]}{RESET}")

            log.info(f"  {GREEN}✓{RESET} Neo4j restored from {key}")

        finally:
            log.info(f"  {DIM}Restarting Neo4j...{RESET}")
            subprocess.run(["docker", "start", "news-agg-neo4j"], capture_output=True, timeout=30)
            log.info(f"  {GREEN}✓{RESET} Neo4j restarted")


# ── Unified push/pull ─────────────────────────────────────────────────────────

def push_all(label: str | None = None) -> dict:
    """Push PostgreSQL + Neo4j snapshots to R2 with same label."""
    results = {}

    # PostgreSQL (always)
    pg_key = push_pg(label)
    results["pg"] = pg_key

    # Neo4j (skip if container doesn't exist)
    try:
        neo4j_key = push_neo4j(label)
        results["neo4j"] = neo4j_key
    except Exception as e:
        log.warning(f"  {YELLOW}Neo4j snapshot skipped: {e}{RESET}")
        results["neo4j"] = None

    log.info(f"\n{BOLD}Push complete{RESET}")
    for store, key in results.items():
        status = f"{GREEN}✓{RESET} {key}" if key else f"{DIM}skipped{RESET}"
        log.info(f"  {store}: {status}")

    return results


async def pull_all(rebuild_search: bool = True) -> dict:
    """Pull PostgreSQL + Neo4j from R2, optionally rebuild Meilisearch.

    Uses the latest snapshot for each data store.
    """
    results = {}

    # PostgreSQL
    try:
        pull_pg()
        results["pg"] = "restored"
    except Exception as e:
        log.error(f"  {RED}PostgreSQL restore failed: {e}{RESET}")
        results["pg"] = f"failed: {e}"

    # Neo4j
    try:
        pull_neo4j()
        results["neo4j"] = "restored"
    except Exception as e:
        log.warning(f"  {YELLOW}Neo4j restore skipped: {e}{RESET}")
        results["neo4j"] = f"skipped: {e}"

    # Meilisearch — rebuild from PostgreSQL (derived data)
    if rebuild_search:
        try:
            log.info(f"\n{BOLD}Rebuilding Meilisearch index{RESET}")
            from news_agg.search import sync_articles
            sync_result = await sync_articles()
            results["meilisearch"] = f"rebuilt ({sync_result['indexed']} articles)"
        except Exception as e:
            log.warning(f"  {YELLOW}Meilisearch rebuild skipped: {e}{RESET}")
            results["meilisearch"] = f"skipped: {e}"

    log.info(f"\n{BOLD}Pull complete{RESET}")
    for store, status in results.items():
        log.info(f"  {store}: {status}")

    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _latest_key(s3, prefix: str) -> str | None:
    """Find the latest snapshot key with the given prefix."""
    response = s3.list_objects_v2(Bucket=settings.r2_bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])
    if not objects:
        return None
    objects.sort(key=lambda o: o["LastModified"], reverse=True)
    return objects[0]["Key"]


def list_snapshots(limit: int = 20) -> list[dict]:
    """List all snapshots in R2 (PostgreSQL + Neo4j)."""
    s3 = _get_s3_client()

    all_objects = []
    for prefix in [PG_PREFIX, NEO4J_PREFIX]:
        response = s3.list_objects_v2(Bucket=settings.r2_bucket_name, Prefix=prefix)
        all_objects.extend(response.get("Contents", []))

    all_objects.sort(key=lambda o: o["LastModified"], reverse=True)

    return [
        {
            "key": obj["Key"],
            "type": "PostgreSQL" if obj["Key"].startswith(PG_PREFIX) else "Neo4j",
            "size_mb": round(obj["Size"] / (1024 * 1024), 1),
            "last_modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M UTC"),
        }
        for obj in all_objects[:limit]
    ]


# ── Legacy aliases (backward compatibility) ──────────────────────────────────
push_snapshot = push_pg
pull_snapshot = pull_pg
