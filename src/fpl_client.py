"""HTTP client for FPL API. Fetches JSON, writes bronze files, returns data + meta. Rate limit + retries."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
DEFAULT_USER_AGENT = "FPL-Pipeline/1.0 (portfolio project; polite crawler)"
FIXED_SLEEP_BETWEEN_CALLS = 0.25
BASE_URL = "https://fantasy.premierleague.com/api/"


def _derive_endpoint_and_request_key(url: str, player_id: Optional[int] = None) -> tuple[str, str]:
    """Derive endpoint name and request_key from URL. For element-summary, pass player_id for request_key."""
    if not url.startswith(BASE_URL):
        rest = url
    else:
        rest = url[len(BASE_URL) :].rstrip("/")
    if rest == "bootstrap-static":
        return "bootstrap-static", "bootstrap-static"
    if rest == "fixtures":
        return "fixtures", "fixtures"
    m = re.match(r"element-summary/(\d+)", rest)
    if m:
        pid = m.group(1)
        return "element-summary", f"element-summary:{pid}"
    m = re.match(r"entry/(\d+)/event/(\d+)/picks", rest)
    if m:
        team_id, gw = m.group(1), m.group(2)
        return "entry", f"entry:{team_id}:{gw}"
    return rest.split("/")[0] if "/" in rest else rest, rest.replace("/", ":")


def _bronze_path(
    bronze_dir: Path,
    endpoint: str,
    request_key: str,
    fetched_at: datetime,
    player_id: Optional[int] = None,
) -> Path:
    """Build bronze file path. Element-summary uses hierarchical {player_id}/{date}/time.json."""
    ts = fetched_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    date_dir = fetched_at.strftime("%Y-%m-%d")
    time_part = fetched_at.strftime("%H%M%S")
    if endpoint == "element-summary" and player_id is not None:
        subdir = bronze_dir / endpoint / str(player_id) / date_dir
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / f"{time_part}.json"
    subdir = bronze_dir / endpoint.replace("/", "-")
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / f"{ts}.json"


class FPLClient:
    """Fetches FPL API, writes bronze JSON, returns (data, meta). Retries on 429/5xx with exponential backoff."""

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        user_agent: str = DEFAULT_USER_AGENT,
        sleep_after_request: float = FIXED_SLEEP_BETWEEN_CALLS,
        max_retries: int = 5,
        backoff_base: float = 2.0,
    ) -> None:
        self.timeout = timeout
        self.headers = {"User-Agent": user_agent}
        self.sleep_after_request = sleep_after_request
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def get_json(
        self,
        url: str,
        bronze_dir: Path,
        player_id: Optional[int] = None,
        save_bronze: bool = True,
        compute_sha256: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        GET url, optionally save response to bronze_dir, return (data, meta).
        meta: endpoint, url, fetched_at_utc, http_status, payload_path, request_key, payload_sha256 (optional).
        """
        endpoint, request_key = _derive_endpoint_and_request_key(url, player_id)
        fetched_at = datetime.now(timezone.utc)

        for attempt in range(self.max_retries):
            try:
                resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            except requests.RequestException as e:
                logger.warning("Request failed (attempt %s): %s", attempt + 1, e)
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.backoff_base ** attempt)
                continue

            if resp.status_code == 429:
                wait = self.backoff_base ** attempt
                logger.info("Rate limited (429), backing off %.1fs", wait)
                time.sleep(wait)
                continue

            if 500 <= resp.status_code < 600:
                logger.warning("Server error %s (attempt %s)", resp.status_code, attempt + 1)
                if attempt == self.max_retries - 1:
                    resp.raise_for_status()
                time.sleep(self.backoff_base ** attempt)
                continue

            resp.raise_for_status()
            data = resp.json()

            payload_path_rel: Optional[str] = None
            payload_sha256: Optional[str] = None

            if save_bronze and bronze_dir:
                path = _bronze_path(bronze_dir, endpoint, request_key, fetched_at, player_id)
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=0, ensure_ascii=False)
                payload_path_rel = str(path)
                logger.info("Saved bronze: %s", payload_path_rel)
                if compute_sha256:
                    payload_sha256 = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

            meta = {
                "endpoint": endpoint,
                "request_key": request_key,
                "url": url,
                "fetched_at_utc": fetched_at,
                "http_status": resp.status_code,
                "payload_path": payload_path_rel,
                "payload_sha256": payload_sha256,
            }
            time.sleep(self.sleep_after_request)
            return data, meta

        raise RuntimeError("Max retries exceeded")

    def sleep_after_request(self) -> None:
        """Call after a request when you want an extra fixed delay (e.g. between per-player calls)."""
        time.sleep(self.sleep_after_request)
