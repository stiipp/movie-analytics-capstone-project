import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = Path(os.environ.get("DATA_DIR", _SCRIPT_DIR.parent.parent / "data"))
PROCESSED_DIR = _DATA_DIR / "processed"


def _running_in_docker() -> bool:
    return Path("/.dockerenv").exists()

SCHEMA = os.environ.get("DB_SCHEMA", "bronze")
DB_HOST = os.environ.get("DB_HOST", "postgres" if _running_in_docker() else "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432" if _running_in_docker() else "5433")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "admin")
DB_NAME = os.environ.get("DB_NAME", "capstone-movie-analytics")


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def ensure_schema(engine, schema_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def load_csv(engine, file_path: Path, table_name: str) -> None:
    if not file_path.exists():
        print(f"[SKIP] Missing file: {file_path}")
        return

    df = pd.read_csv(file_path)
    df.to_sql(table_name, con=engine, schema=SCHEMA, if_exists="replace", index=False)
    print(f"[OK] Loaded {len(df):,} rows -> {SCHEMA}.{table_name}")


def load_ratings_json(engine, file_path: Path, table_name: str) -> None:
    if not file_path.exists():
        print(f"[SKIP] Missing file: {file_path}")
        return

    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = []
    for record in payload:
        summary = record.get("ratings_summary") or {}
        rows.append(
            {
                "movie_id": record.get("movie_id"),
                "avg_rating": summary.get("avg_rating"),
                "total_ratings": summary.get("total_ratings"),
                "std_dev": summary.get("std_dev"),
                "last_rated": record.get("last_rated"),
            }
        )

    df = pd.DataFrame(rows)
    df.to_sql(table_name, con=engine, schema=SCHEMA, if_exists="replace", index=False)
    print(f"[OK] Loaded {len(df):,} rows -> {SCHEMA}.{table_name}")


def main() -> None:
    engine = get_engine()
    ensure_schema(engine, SCHEMA)

    load_csv(engine, PROCESSED_DIR / "movies_main.csv", "movies_main")
    load_csv(engine, PROCESSED_DIR / "movie_extended.csv", "movie_extended")
    load_ratings_json(engine, PROCESSED_DIR / "ratings.json", "ratings")

    print("[DONE] Bronze ingestion completed.")


if __name__ == "__main__":
    main()
