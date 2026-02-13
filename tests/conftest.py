"""Pytest fixtures. Fixture JSON lives in tests/fixtures/ (endpoint-specific names)."""

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def bootstrap_static_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "bootstrap_static_sample.json"


@pytest.fixture
def element_summary_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "element_summary_sample.json"


@pytest.fixture
def fixtures_sample_path(fixtures_dir: Path) -> Path:
    return fixtures_dir / "fixtures_sample.json"
