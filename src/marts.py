"""Analytics-friendly SQLite views. now_cost is in tenths (55 = £5.5); views expose now_cost_million."""

from __future__ import annotations

import logging
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def init_marts(engine: Engine) -> None:
    """Create views if not exists. Call after init_db."""
    with engine.connect() as conn:
        # players joined with teams + positions; now_cost_million = now_cost/10.0 (FPL stores in tenths)
        conn.execute(
            text("""
            CREATE VIEW IF NOT EXISTS v_player_latest AS
            SELECT
                p.id,
                p.web_name,
                p.first_name,
                p.second_name,
                p.team_id,
                t.name AS team_name,
                t.short_name AS team_short_name,
                p.element_type_id,
                e.singular_name_short AS position_short,
                e.singular_name AS position_name,
                p.now_cost,
                CAST(p.now_cost AS REAL) / 10.0 AS now_cost_million,
                p.status,
                p.minutes,
                p.total_points,
                p.selected_by_percent,
                p.form,
                p.points_per_game,
                p.expected_goals,
                p.expected_assists,
                p.ingested_at_utc
            FROM players p
            LEFT JOIN teams t ON p.team_id = t.id
            LEFT JOIN element_types e ON p.element_type_id = e.id
            """)
        )
        conn.commit()

        # Next N fixtures by kickoff_time (unfinished)
        conn.execute(
            text("""
            CREATE VIEW IF NOT EXISTS v_fixture_upcoming AS
            SELECT
                f.id,
                f.event_id,
                ev.name AS event_name,
                f.team_h,
                f.team_a,
                th.short_name AS team_h_short,
                ta.short_name AS team_a_short,
                f.kickoff_time,
                f.finished,
                f.team_h_difficulty,
                f.team_a_difficulty
            FROM fixtures f
            LEFT JOIN events ev ON f.event_id = ev.id
            LEFT JOIN teams th ON f.team_h = th.id
            LEFT JOIN teams ta ON f.team_a = ta.id
            WHERE f.finished = 0 OR f.finished IS NULL
            """)
        )
        conn.commit()

        # Rolling minutes/points last 5 games per player (from match history)
        conn.execute(
            text("""
            CREATE VIEW IF NOT EXISTS v_player_form AS
            WITH ranked AS (
                SELECT
                    player_id,
                    event_id,
                    minutes,
                    total_points,
                    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY event_id DESC) AS rn
                FROM player_match_history
                WHERE event_id IS NOT NULL
            ),
            last5 AS (
                SELECT player_id, minutes, total_points, rn
                FROM ranked
                WHERE rn <= 5
            )
            SELECT
                player_id,
                COUNT(*) AS games_last5,
                SUM(minutes) AS minutes_last5,
                SUM(total_points) AS points_last5,
                CAST(SUM(total_points) AS REAL) / NULLIF(COUNT(*), 0) AS ppg_last5
            FROM last5
            GROUP BY player_id
            """)
        )
        conn.commit()

    logger.info("Analytics views created or updated (v_player_latest, v_fixture_upcoming, v_player_form)")
