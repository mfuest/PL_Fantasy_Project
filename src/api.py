"""Single endpoint: accept (team_id, gw, bank) → fetch squad from FPL API → top 10 transfer suggestions + team xPts."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)


def _get_next_event_id(engine: Any) -> int | None:
    """Min event_id from player_expected_points (next GW we have xPts for)."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MIN(event_id) FROM player_expected_points")
        ).fetchone()
    return row[0] if row and row[0] is not None else None


def _suggestions_response(current_team_xpts: float, suggestions: list[Any]) -> dict[str, Any]:
    """Build JSON-serializable response shape."""
    return {
        "current_team_xpts": current_team_xpts,
        "suggestions": [
            {
                "out_player_id": s.out_player_id,
                "in_player_id": s.in_player_id,
                "out_web_name": s.out_web_name,
                "in_web_name": s.in_web_name,
                "team_xpts_delta": s.team_xpts_delta,
                "new_team_xpts": s.new_team_xpts,
                "cost_delta_million": s.cost_delta_million,
            }
            for s in suggestions
        ],
    }


def create_app(engine: Any, client: Any, bronze_dir: Path | None = None) -> Any:
    """Create FastAPI app with dependencies (engine, client, bronze_dir)."""
    try:
        from fastapi import FastAPI, Form, HTTPException
        from fastapi.responses import FileResponse, JSONResponse
    except ImportError:
        raise ImportError("Install API extras: pip install -e '.[api]'") from None

    from src.squad_source import get_squad_from_api
    from src.transfer_engine import run_transfer_engine

    from src.db import init_db
    from src.marts import init_marts

    app = FastAPI(title="FPL Transfer Suggestions", version="0.1.0")
    _bronze = bronze_dir or Path("data/bronze")

    # Project root: parent of src/
    _project_root = Path(__file__).resolve().parent.parent
    _static_dir = _project_root / "static"
    _index_path = _static_dir / "index.html"

    @app.get("/")
    async def index():
        """Serve the transfer suggestions web UI."""
        if _index_path.exists():
            return FileResponse(_index_path, media_type="text/html")
        raise HTTPException(status_code=404, detail="Static files not found")

    @app.post("/suggestions")
    async def suggestions(
        team_id: int = Form(...),
        gw: int = Form(...),
        bank: float | None = Form(default=None),
        top_n: int = Form(default=10),
    ):
        """Return top transfer suggestions and team xPts.

        Provide team_id + gw (and optional bank). Fetches squad from FPL entry picks API → transfer engine.

        Response: current_team_xpts, suggestions (list of out/in, team_xpts_delta, new_team_xpts, cost_delta_million).
        """
        init_db(engine)
        init_marts(engine)
        bank_million = (float(bank) if bank is not None else None) or 0.0
        event_id = gw

        try:
            squad_ids, api_bank = get_squad_from_api(
                client, team_id, gw, save_bronze=False, bronze_dir=_bronze
            )
            if bank_million == 0.0 and api_bank != 0.0:
                bank_million = api_bank
        except Exception as e:
            logger.exception("Failed to fetch squad from FPL API")
            # FPL returns 404 when picks for this gameweek aren't available yet
            if getattr(e, "response", None) is not None and getattr(e.response, "status_code", None) == 404:
                raise HTTPException(
                    status_code=404,
                    detail="Picks for this gameweek aren't available from FPL yet. Try the current or next gameweek.",
                ) from e
            raise HTTPException(status_code=502, detail=f"FPL API error: {e}") from e
        if len(squad_ids) != 15:
            raise HTTPException(
                status_code=400,
                detail=f"Expected 15 picks; got {len(squad_ids)}.",
            )
        if _get_next_event_id(engine) is None:
            raise HTTPException(
                status_code=503,
                detail="No expected points data; run build_xpts first.",
            )

        current_team_xpts, suggestions_list = run_transfer_engine(
            engine, squad_ids, event_id, bank_million=bank_million, top_n=max(1, min(50, top_n))
        )
        return JSONResponse(_suggestions_response(current_team_xpts, suggestions_list))

    return app


def run_api(
    host: str = "0.0.0.0",
    port: int = 8000,
    db_path: str = "data/fpl.sqlite",
    bronze_dir: str | None = None,
) -> None:
    """Run the API server (uvicorn)."""
    try:
        import uvicorn
    except ImportError:
        raise ImportError("Install API extras: pip install -e '.[api]'") from None
    from src.db import get_engine
    from src.fpl_client import FPLClient

    engine = get_engine(db_path)
    client = FPLClient()
    app = create_app(engine, client, Path(bronze_dir) if bronze_dir else None)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="FPL Transfer Suggestions API")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db-path", default="data/fpl.sqlite")
    parser.add_argument("--bronze-dir", default=None)
    args = parser.parse_args()
    run_api(host=args.host, port=args.port, db_path=args.db_path, bronze_dir=args.bronze_dir)
    sys.exit(0)
