from __future__ import annotations

from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import DATABASE_URL


def get_engine() -> Engine:
    Path("data").mkdir(parents=True, exist_ok=True)
    engine = create_engine(DATABASE_URL, future=True)
    return engine


def init_db(engine: Engine | None = None) -> None:
    engine = engine or get_engine()
    schema_path = Path(__file__).resolve().parent.parent.parent / "sql" / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    with engine.begin() as conn:
        for stmt in schema_sql.split(";\n"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
