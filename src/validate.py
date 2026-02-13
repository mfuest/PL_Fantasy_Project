"""Data quality checks with severity split: hard fail vs warn. Exit behavior controlled by --level."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ----- Hard fail bounds -----
MINUTES_HARD_LO = 0
MINUTES_HARD_HI = 130  # Hard fail if minutes > 130 or < 0

# ----- Warn / anomaly reporting bounds -----
MIN_PLAYERS = 600
FIXTURES_MIN = 300
FIXTURES_MAX = 400
TOTAL_POINTS_ANOMALY_LO = -10
TOTAL_POINTS_ANOMALY_HI = 40
WEIRD_ROWS_LIMIT = 10

ValidationLevel = Literal["hard", "strict", "warn"]


@dataclass
class ValidationReport:
    """Report: counts, null pct, weird rows, hard errors, and warnings."""

    counts: dict[str, int] = field(default_factory=dict)
    null_pct: dict[str, dict[str, float]] = field(default_factory=dict)
    weird_rows: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)  # Hard failures
    warnings: list[str] = field(default_factory=list)  # Warn-level only

    def fail(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def is_ok(self) -> bool:
        """True if no hard failures."""
        return len(self.errors) == 0

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


def run_validation(engine: Engine) -> ValidationReport:
    """Run all data quality checks. Populates report; use is_ok() / has_warnings() and --level for exit."""
    r = ValidationReport()

    with engine.connect() as conn:
        # ----- Counts by table (and missing core tables = hard fail) -----
        core_tables = (
            "teams",
            "element_types",
            "events",
            "players",
            "fixtures",
            "player_match_history",
            "player_future_fixtures",
            "player_expected_points",
        )
        for table in core_tables:
            try:
                row = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                r.counts[table] = row[0] if row else 0
            except Exception as e:
                r.fail(f"Missing or inaccessible table {table}: {e}")
                r.counts[table] = 0

        # Hard: missing core tables (zero rows can be valid for some; "missing" = exception above)
        # Warn: row count ranges
        if r.counts.get("players", 0) <= MIN_PLAYERS:
            r.warn(
                f"players.count={r.counts.get('players', 0)} (expected > {MIN_PLAYERS})"
            )
        if not (FIXTURES_MIN <= r.counts.get("fixtures", 0) <= FIXTURES_MAX):
            r.warn(
                f"fixtures.count={r.counts.get('fixtures', 0)} (expected {FIXTURES_MIN}-{FIXTURES_MAX})"
            )

        # ----- Null PKs / key columns (hard) -----
        _check_nulls(conn, r)

        # ----- player_match_history: minutes [0,130] hard; total_points anomaly warn only -----
        _check_match_history_ranges(conn, r)

        # ----- Referential integrity (hard) -----
        _check_referential_integrity(conn, r)

        # ----- Top weird rows for anomaly reporting (bounds [-10, 40] and [0, 130]) -----
        _collect_weird_rows(conn, r)

    return r


def _check_nulls(conn: Any, r: ValidationReport) -> None:
    """Percent null in key columns. Null PKs/key columns = hard fail."""
    key_columns = [
        ("players", "id", "players.id"),
        ("players", "team_id", "players.team_id"),
        ("fixtures", "id", "fixtures.id"),
        ("fixtures", "event_id", "fixtures.event_id"),  # can be null for postponed
        ("player_match_history", "player_id", "player_match_history.player_id"),
        (
            "player_match_history",
            "fixture_id_effective",
            "player_match_history.fixture_id_effective",
        ),
    ]
    for table, col, label in key_columns:
        try:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            if total == 0:
                continue
            nulls = (
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                ).scalar()
                or 0
            )
            pct = 100.0 * nulls / total
            if label not in r.null_pct:
                r.null_pct[label] = {}
            r.null_pct[label]["pct_null"] = round(pct, 2)
            if col in ("id", "player_id", "fixture_id_effective") and nulls > 0:
                r.fail(f"Unexpected nulls in {label}: {nulls} ({pct:.1f}%)")
            if col == "team_id" and pct > 0:
                r.fail(f"players.team_id has {nulls} nulls ({pct:.1f}%)")
        except Exception as e:
            r.fail(f"null check {label}: {e}")


def _check_match_history_ranges(conn: Any, r: ValidationReport) -> None:
    """Hard: minutes outside [0, 130]. Warn: total_points outside [-10, 40]."""
    bad_minutes = (
        conn.execute(
            text(
                "SELECT COUNT(*) FROM player_match_history WHERE minutes IS NOT NULL AND (minutes < :lo OR minutes > :hi)"
            ),
            {"lo": MINUTES_HARD_LO, "hi": MINUTES_HARD_HI},
        ).scalar()
        or 0
    )
    if bad_minutes > 0:
        r.fail(
            f"player_match_history: {bad_minutes} rows with minutes outside [{MINUTES_HARD_LO},{MINUTES_HARD_HI}]"
        )

    bad_points = (
        conn.execute(
            text(
                "SELECT COUNT(*) FROM player_match_history WHERE total_points IS NOT NULL AND (total_points < :lo OR total_points > :hi)"
            ),
            {"lo": TOTAL_POINTS_ANOMALY_LO, "hi": TOTAL_POINTS_ANOMALY_HI},
        ).scalar()
        or 0
    )
    if bad_points > 0:
        r.warn(
            f"player_match_history: {bad_points} rows with total_points outside [{TOTAL_POINTS_ANOMALY_LO},{TOTAL_POINTS_ANOMALY_HI}] (anomaly)"
        )


def _check_referential_integrity(conn: Any, r: ValidationReport) -> None:
    orphan_team = (
        conn.execute(
            text(
                "SELECT COUNT(*) FROM players p LEFT JOIN teams t ON p.team_id = t.id WHERE p.team_id IS NOT NULL AND t.id IS NULL"
            )
        ).scalar()
        or 0
    )
    if orphan_team > 0:
        r.fail(
            f"Referential integrity: {orphan_team} players with team_id not in teams"
        )

    orphan_event = (
        conn.execute(
            text(
                "SELECT COUNT(*) FROM fixtures f LEFT JOIN events e ON f.event_id = e.id WHERE f.event_id IS NOT NULL AND e.id IS NULL"
            )
        ).scalar()
        or 0
    )
    if orphan_event > 0:
        r.fail(
            f"Referential integrity: {orphan_event} fixtures with event_id not in events"
        )


def _collect_weird_rows(conn: Any, r: ValidationReport) -> None:
    """Top N weird rows for anomaly reporting: minutes [0,130], total_points [-10,40]."""
    weird = conn.execute(
        text("""
            SELECT player_id, fixture_id_effective, minutes, total_points
            FROM player_match_history
            WHERE (minutes IS NOT NULL AND (minutes < :min_lo OR minutes > :min_hi))
               OR (total_points IS NOT NULL AND (total_points < :pts_lo OR total_points > :pts_hi))
            LIMIT :lim
        """),
        {
            "min_lo": MINUTES_HARD_LO,
            "min_hi": MINUTES_HARD_HI,
            "pts_lo": TOTAL_POINTS_ANOMALY_LO,
            "pts_hi": TOTAL_POINTS_ANOMALY_HI,
            "lim": WEIRD_ROWS_LIMIT,
        },
    ).fetchall()
    for row in weird:
        r.weird_rows.append(
            {
                "player_id": row[0],
                "fixture_id_effective": row[1],
                "minutes": row[2],
                "total_points": row[3],
            }
        )


def should_exit_nonzero(level: ValidationLevel, report: ValidationReport) -> bool:
    """Return True if the pipeline should exit with code 1 for this level and report."""
    if level == "hard":
        return not report.is_ok()
    if level == "strict":
        return not report.is_ok() or report.has_warnings()
    # warn: never exit 1
    return False


def print_report(r: ValidationReport) -> None:
    """Log report: counts, null pct, weird rows, errors, warnings."""
    logger.info("=== Validation report ===")
    logger.info("Counts: %s", r.counts)
    if r.null_pct:
        logger.info("Null %% (key columns): %s", r.null_pct)
    if r.weird_rows:
        logger.info(
            "Top %s weird rows (minutes/points anomaly): %s",
            WEIRD_ROWS_LIMIT,
            r.weird_rows,
        )
    for e in r.errors:
        logger.error("Validation error (hard): %s", e)
    for w in r.warnings:
        logger.warning("Validation warning: %s", w)
