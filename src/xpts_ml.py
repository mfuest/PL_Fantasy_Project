"""ML-based expected points (XGBoost GBM). FPL silver only; coexists with baseline.

Requires: pip install -e '.[ml]'
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Feature columns used in training and inference (must match).
FEATURE_COLUMNS = [
    "element_type_id",
    "team_strength",
    "opponent_strength",
    "is_home",
    "difficulty",
    "form",
    "points_per_game",
    "minutes_per_game",
    "status_available",
]

# Position splits for xpts_att / xpts_def from total xpts (same as baseline).
ELEMENT_TYPE_GKP, ELEMENT_TYPE_DEF, ELEMENT_TYPE_MID, ELEMENT_TYPE_FWD = 1, 2, 3, 4


def _get_att_def_fractions(element_type_id: int | None) -> tuple[float, float]:
    if element_type_id in (ELEMENT_TYPE_GKP, ELEMENT_TYPE_DEF):
        return 0.4, 0.6
    if element_type_id == ELEMENT_TYPE_MID:
        return 0.7, 0.3
    return 0.9, 0.1


def build_training_data(engine: Engine) -> tuple[Any, Any]:
    """Build feature matrix X and target y from player_match_history + players + fixtures + teams.

    Returns (X, y) where X is a DataFrame with FEATURE_COLUMNS and y is Series of total_points.
    Uses current player/form snapshot (no point-in-time form); fixture gives difficulty, is_home, strengths.
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("Install ML extras: pip install -e '.[ml]'") from None

    with engine.connect() as conn:
        # One row per (player, fixture): join history to fixtures and players; derive is_home, difficulty, strengths.
        df = pd.read_sql(
            text("""
            SELECT
                pmh.player_id,
                pmh.event_id,
                pmh.total_points,
                p.team_id,
                p.element_type_id,
                p.form,
                p.points_per_game,
                p.minutes,
                p.status,
                f.team_h,
                f.team_a,
                f.team_h_difficulty,
                f.team_a_difficulty,
                t.strength AS team_strength,
                CASE WHEN p.team_id = f.team_h THEN t_a.strength ELSE t_h.strength END AS opponent_strength
            FROM player_match_history pmh
            INNER JOIN fixtures f ON f.id = pmh.fixture_id_effective
            INNER JOIN players p ON p.id = pmh.player_id
            INNER JOIN teams t ON t.id = p.team_id
            LEFT JOIN teams t_h ON t_h.id = f.team_h
            LEFT JOIN teams t_a ON t_a.id = f.team_a
            WHERE pmh.total_points IS NOT NULL
              AND p.team_id IS NOT NULL
              AND f.team_h IS NOT NULL
              AND f.team_a IS NOT NULL
            """),
            conn,
        )

    if df.empty:
        empty_X = pd.DataFrame(columns=FEATURE_COLUMNS)
        empty_y = pd.Series(dtype=float)
        return empty_X, empty_y

    # Derived features
    df["is_home"] = (df["team_id"] == df["team_h"]).astype(int)
    df["difficulty"] = df.apply(
        lambda r: r["team_h_difficulty"] if r["team_id"] == r["team_h"] else r["team_a_difficulty"],
        axis=1,
    )
    df["status_available"] = (df["status"].fillna("").str.strip().str.lower() == "a").astype(int)
    # Minutes per game: use season minutes / 38 as proxy for "games" if no event count
    df["minutes_per_game"] = (df["minutes"].fillna(0) / 38.0).clip(upper=90)

    df["team_strength"] = df["team_strength"].fillna(3)
    df["opponent_strength"] = df["opponent_strength"].fillna(3)
    df["form"] = df["form"].fillna(0.0)
    df["points_per_game"] = df["points_per_game"].fillna(0.0)
    df["difficulty"] = df["difficulty"].fillna(3).astype(int).clip(1, 5)
    df["element_type_id"] = df["element_type_id"].fillna(3).astype(int)

    X = df[FEATURE_COLUMNS].copy()
    y = df["total_points"]
    return X, y


def train_model(
    engine: Engine,
    model_path: str | Path,
    *,
    validation_fraction: float = 0.2,
    random_state: int = 0,
) -> None:
    """Train XGBoost regressor on build_training_data(engine), save to model_path."""
    try:
        import xgboost as xgb
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error
    except ImportError:
        raise ImportError("Install ML extras: pip install -e '.[ml]'") from None

    X, y = build_training_data(engine)
    if len(X) < 10:
        raise ValueError(
            "Not enough training rows (need at least 10). Run update_core and update_element_summaries, then retry."
        )

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=validation_fraction, random_state=random_state
    )
    model = xgb.XGBRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=random_state,
    )
    model.fit(X_train, y_train)
    pred_val = model.predict(X_val)
    mae = mean_absolute_error(y_val, pred_val)
    logger.info("xPts ML validation MAE: %.3f (n_train=%s, n_val=%s)", mae, len(X_train), len(X_val))

    path = Path(model_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # XGBoost saves as .json if we use save_model; use .json extension for clarity
    out = path if path.suffix else path.with_suffix(".json")
    model.save_model(str(out))
    logger.info("Saved model to %s", out)


def build_xpts_rows_ml(engine: Engine, horizon: int, model_path: str | Path) -> list[dict[str, Any]]:
    """Build expected points rows using the ML model; same shape as xpts.build_xpts_rows.

    Uses baseline compute_xmins from xpts.py; xpts from GBM; xpts_att/def/app split by position from predicted xpts.
    """
    try:
        import xgboost as xgb
        import pandas as pd
    except ImportError:
        raise ImportError("Install ML extras: pip install -e '.[ml]'") from None

    from src.xpts import (
        FormRow,
        PlayerRow,
        _fetch_difficulty_for_team_event,
        _fetch_finished_events_count,
        _fetch_form_by_player,
        _fetch_upcoming_event_ids,
        _fetch_players,
        compute_xmins,
    )

    path = Path(model_path)
    if not path.exists():
        candidate = path.with_suffix(".json")
        if candidate.exists():
            path = candidate
        else:
            raise FileNotFoundError(
                f"Model file not found at {model_path}. Run: python -m src.pipeline train_xpts"
            )
    model = xgb.XGBRegressor()
    model.load_model(str(path))

    computed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows_out: list[dict[str, Any]] = []

    with engine.connect() as conn:
        event_ids = _fetch_upcoming_event_ids(conn, horizon)
        if not event_ids:
            logger.warning("No upcoming event_ids found; run update_core and ensure fixtures have event_id")
            return rows_out

        finished_count = _fetch_finished_events_count(conn)
        form_by_player = _fetch_form_by_player(conn)
        players = _fetch_players(conn)
        diff_map = _fetch_difficulty_for_team_event(conn)

        # Team strengths and upcoming fixture (team_h, team_a) per (team_id, event_id)
        teams_strength: dict[int, int] = {}
        team_event_fixture: dict[tuple[int, int], tuple[int, int, int]] = {}  # (team_id, event_id) -> (team_h, team_a, difficulty)
        fixture_rows = conn.execute(
            text("""
            SELECT event_id, team_h, team_a, team_h_difficulty, team_a_difficulty
            FROM v_fixture_upcoming
            WHERE event_id IS NOT NULL AND team_h IS NOT NULL AND team_a IS NOT NULL
            """)
        ).fetchall()
        for r in fixture_rows:
            ev, th, ta, dh, da = r[0], r[1], r[2], r[3], r[4]
            if th is not None:
                team_event_fixture[(th, ev)] = (th, ta, dh if dh is not None else 3)
            if ta is not None:
                team_event_fixture[(ta, ev)] = (th, ta, da if da is not None else 3)

        team_rows = conn.execute(text("SELECT id, strength FROM teams")).fetchall()
        for r in team_rows:
            teams_strength[r[0]] = r[1] if r[1] is not None else 3

        player_form_fpl: dict[int, float] = {}
        for r in conn.execute(text("SELECT id, form FROM players")).fetchall():
            player_form_fpl[r[0]] = float(r[1]) if r[1] is not None else 0.0

    for player in players:
        if player.team_id is None:
            continue
        form = form_by_player.get(player.player_id)
        xmins = compute_xmins(form, player, finished_count)

        for event_id in event_ids:
            key = (player.team_id, event_id)
            if key not in team_event_fixture:
                difficulty = 3
                is_home = 0
                opponent_strength = 3
            else:
                th, ta, difficulty = team_event_fixture[key]
                is_home = 1 if player.team_id == th else 0
                opponent_team = ta if is_home else th
                opponent_strength = teams_strength.get(opponent_team, 3)

            team_strength = teams_strength.get(player.team_id, 3)
            status_available = 1 if (player.status or "").strip().lower() == "a" else 0
            minutes_pg = min(90.0, (player.minutes or 0) / 38.0)

            X_row = pd.DataFrame([{
                "element_type_id": player.element_type_id or 3,
                "team_strength": team_strength,
                "opponent_strength": opponent_strength,
                "is_home": is_home,
                "difficulty": difficulty,
                "form": player_form_fpl.get(player.player_id, 0.0),
                "points_per_game": player.points_per_game or 0.0,
                "minutes_per_game": minutes_pg,
                "status_available": status_available,
            }])
            xpts = float(model.predict(X_row[FEATURE_COLUMNS])[0])
            xpts = max(0.0, round(xpts, 2))

            att_frac, def_frac = _get_att_def_fractions(player.element_type_id)
            xpts_att = round(xpts * att_frac, 2)
            xpts_def = round(xpts * def_frac, 2)
            xpts_app = 0.0  # plan: simple split; no baseline appearance component

            rows_out.append({
                "player_id": player.player_id,
                "event_id": event_id,
                "xmins": round(xmins, 2),
                "xpts": xpts,
                "xpts_att": xpts_att,
                "xpts_def": xpts_def,
                "xpts_app": xpts_app,
                "computed_at_utc": computed_at,
            })

    return rows_out
