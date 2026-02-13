"""Squad source: fetch 15 player IDs + bank from FPL API (team_id + gw)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.normalize import normalize_entry_picks

if TYPE_CHECKING:
    from src.fpl_client import FPLClient

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://fantasy.premierleague.com/api/"


def get_squad_from_api(
    client: FPLClient,
    team_id: int,
    gw: int,
    base_url: str = DEFAULT_BASE_URL,
    save_bronze: bool = False,
    bronze_dir: str | None = None,
) -> tuple[list[int], float]:
    """Fetch entry picks for team_id and gameweek; return (15 player IDs, bank in millions).

    Uses FPL API entry/{team_id}/event/{gw}/picks/. Bank from entry_history if present,
    else 0.0. Does not require DB.
    """
    from pathlib import Path

    url = f"{base_url}entry/{team_id}/event/{gw}/picks/"
    if bronze_dir is None:
        bronze_dir = Path("data/bronze")
    else:
        bronze_dir = Path(bronze_dir)
    data, _ = client.get_json(url, bronze_dir, save_bronze=save_bronze)
    parsed = normalize_entry_picks(data)
    picks = parsed.get("picks") or []
    player_ids = [p["element"] for p in picks if p.get("element") is not None]
    eh = parsed.get("entry_history") or {}
    bank_raw = eh.get("bank")  # FPL API: bank in tenths (e.g. 5 = £0.5m)
    if bank_raw is not None:
        try:
            bank_million = int(bank_raw) / 10.0
        except (TypeError, ValueError):
            bank_million = 0.0
    else:
        bank_million = 0.0
    return player_ids, bank_million
