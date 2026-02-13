"""SQLAlchemy ORM models for FPL silver layer. All silver tables use ingested_at_utc."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all models."""

    pass


def _utc_now() -> datetime:
    return datetime.utcnow()


class MetaIngestion(Base):
    """Append-only log of each API fetch. run_id groups fetches from the same pipeline run."""

    __tablename__ = "meta_ingestions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True, index=True)
    request_key: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, index=True)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    etag: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    payload_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_meta_ingestions_endpoint_fetched", "endpoint", "fetched_at_utc"),
        Index("ix_meta_ingestions_request_key_fetched", "request_key", "fetched_at_utc"),
    )


class Team(Base):
    __tablename__ = "teams"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    short_name: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    strength: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)


class ElementType(Base):
    __tablename__ = "element_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    singular_name_short: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    singular_name: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    squad_select: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    deadline_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    finished: Mapped[Optional[bool]] = mapped_column(nullable=True)
    is_current: Mapped[Optional[bool]] = mapped_column(nullable=True)
    is_next: Mapped[Optional[bool]] = mapped_column(nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)


class Player(Base):
    __tablename__ = "players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    web_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    second_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    team_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("teams.id"), nullable=True, index=True)
    element_type_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("element_types.id"), nullable=True, index=True
    )
    now_cost: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, index=True
    )  # FPL stores in tenths (e.g. 55 = £5.5); use v_player_latest.now_cost_million for display
    status: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    total_points: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    selected_by_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    form: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    points_per_game: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_goals: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_assists: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_goal_involvements: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_goals_conceded: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)


class Fixture(Base):
    __tablename__ = "fixtures"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    team_h: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("teams.id"), nullable=True, index=True)
    team_a: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("teams.id"), nullable=True, index=True)
    kickoff_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    finished: Mapped[Optional[bool]] = mapped_column(nullable=True)
    team_h_difficulty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    team_a_difficulty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_fixtures_event_teams", "event_id", "team_h", "team_a"),
        Index("ix_fixtures_kickoff_time", "kickoff_time"),
    )


class PlayerMatchHistory(Base):
    """Per-player per-fixture history. PK is (player_id, fixture_id_effective); fixture_id is nullable from API."""

    __tablename__ = "player_match_history"

    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("players.id"), primary_key=True)
    fixture_id_effective: Mapped[int] = mapped_column(Integer, primary_key=True)
    fixture_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    event_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    total_points: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    goals_scored: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    assists: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    clean_sheets: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    goals_conceded: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expected_goals: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_assists: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_goal_involvements: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    expected_goals_conceded: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)

    __table_args__ = (Index("ix_player_match_history_player_event", "player_id", "event_id"),)


class PlayerFutureFixture(Base):
    """Upcoming fixtures per player. Minimal: player_id, fixture_id (FK), plus player-specific fields (e.g. difficulty)."""

    __tablename__ = "player_future_fixtures"

    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("players.id"), primary_key=True)
    fixture_id: Mapped[int] = mapped_column(Integer, ForeignKey("fixtures.id"), primary_key=True)
    event_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_home: Mapped[Optional[bool]] = mapped_column(nullable=True)
    opponent_team: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    kickoff_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=False), nullable=True)
    difficulty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ingested_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)


class PlayerExpectedPoints(Base):
    """Baseline expected FPL points per player per upcoming gameweek. No ML; minutes + fixture + form."""

    __tablename__ = "player_expected_points"

    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("players.id"), primary_key=True)
    event_id: Mapped[int] = mapped_column(Integer, ForeignKey("events.id"), primary_key=True)
    xmins: Mapped[float] = mapped_column(Float, nullable=False)
    xpts: Mapped[float] = mapped_column(Float, nullable=False)
    xpts_att: Mapped[float] = mapped_column(Float, nullable=False)
    xpts_def: Mapped[float] = mapped_column(Float, nullable=False)
    xpts_app: Mapped[float] = mapped_column(Float, nullable=False)
    computed_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, default=_utc_now)
