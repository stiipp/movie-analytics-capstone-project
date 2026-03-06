"""
Pandas cleaning pipeline — transforms bronze (raw) data into silver_base.

Responsibilities:
  - Deduplicate rows
  - Parse mixed date formats
  - Cast numeric columns
  - Flatten nested JSON (ratings)
  - Standardize nulls (zeros → NULL where semantically missing)
  - Write cleaned tables to the silver_base schema in Postgres

Usage:
    python scripts/processing/clean_pandas.py
"""

import json
import logging
import os
import sys
import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
_DATA_DIR = Path(os.environ.get("DATA_DIR", _PROJECT_ROOT / "data"))
PROCESSED_DIR = _DATA_DIR / "processed"

# ---------------------------------------------------------------------------
# DB config (auto-detects host vs Docker)
# ---------------------------------------------------------------------------

def _running_in_docker() -> bool:
    return Path("/.dockerenv").exists()

SCHEMA = os.environ.get("DB_SCHEMA", "silver_base")
DB_HOST = os.environ.get("DB_HOST", "postgres" if _running_in_docker() else "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432" if _running_in_docker() else "5433")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "admin")
DB_NAME = os.environ.get("DB_NAME", "capstone-movie-analytics")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("clean_pandas")

# ---------------------------------------------------------------------------
# Date formats detected in the dataset (3 mixed formats)
# ---------------------------------------------------------------------------
DATE_FORMATS = [
    "%Y-%m-%d",   # 2016-04-22
    "%m/%d/%Y",   # 11/01/1996
    "%d-%m-%Y",   # 17-10-2008
]


def require_file(file_path: Path) -> Path:
    """Return file_path if present, otherwise fail fast with a clear error."""
    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        sys.exit(1)
    return file_path


def normalize_id_column(df: pd.DataFrame, column: str = "id") -> pd.DataFrame:
    """Coerce id-like columns to int and drop invalid values."""
    out = df.copy()
    out[column] = pd.to_numeric(out[column], errors="coerce")
    bad = out[column].isnull().sum()
    if bad:
        logger.warning("Dropped %d rows with non-numeric %s.", bad, column)
        out = out.dropna(subset=[column])
    out[column] = out[column].astype(int)
    return out


def parse_mixed_date_series(series: pd.Series) -> pd.Series:
    """Parse mixed-format date strings using known formats in vectorized passes."""
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    raw = series.astype("string").str.strip()

    for fmt in DATE_FORMATS:
        mask = parsed.isna() & raw.notna()
        if mask.any():
            parsed.loc[mask] = pd.to_datetime(raw.loc[mask], format=fmt, errors="coerce")

    return parsed


def parse_stringified_dict_list(raw: str, key: str) -> str | None:
    """Extract values from a stringified list of dicts, return comma-separated string."""
    if pd.isna(raw):
        return None
    try:
        items = ast.literal_eval(raw)
        if isinstance(items, list):
            return ",".join(item[key] for item in items if isinstance(item, dict) and key in item)
    except (ValueError, SyntaxError):
        pass
    return raw

# ===================================================================
# MovieProcessor
# ===================================================================

class MovieProcessor:
    """Cleans and standardises the movies_main dataset."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        self._initial_rows = len(self.df)
        logger.info("MovieProcessor initialised with %d rows.", self._initial_rows)

    # --- public pipeline --------------------------------------------------

    def process(self) -> pd.DataFrame:
        """Run the full cleaning pipeline and return the cleaned DataFrame."""
        self._deduplicate()
        self._cast_id()
        self._standardize_release_date()
        self._cast_financials()
        self._add_flags()
        logger.info(
            "MovieProcessor finished: %d → %d rows.",
            self._initial_rows,
            len(self.df),
        )
        return self.df

    # --- private steps ----------------------------------------------------

    def _deduplicate(self) -> None:
        before = len(self.df)
        self.df = self.df.drop_duplicates(subset=["id"], keep="first")
        dropped = before - len(self.df)
        if dropped:
            logger.info("Dropped %d duplicate movie rows.", dropped)

    def _cast_id(self) -> None:
        self.df = normalize_id_column(self.df, "id")

    def _standardize_release_date(self) -> None:
        self.df["release_date"] = parse_mixed_date_series(self.df["release_date"])
        unparsed = self.df["release_date"].isnull().sum()
        logger.info(
            "Parsed release_date: %d valid, %d null.",
            len(self.df) - unparsed,
            unparsed,
        )

    def _cast_financials(self) -> None:
        for col in ("budget", "revenue"):
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
            # Treat 0 as unreported (NULL) for financial columns
            self.df[col] = self.df[col].where(self.df[col] > 0, other=None)
            nulls = self.df[col].isnull().sum()
            logger.info(
                "%s: %d valid, %d null (includes zeros treated as missing).",
                col,
                len(self.df) - nulls,
                nulls,
            )

    def _add_flags(self) -> None:
        self.df["is_budget_reported"] = self.df["budget"].notna()
        self.df["is_revenue_reported"] = self.df["revenue"].notna()
        self.df["is_roi_eligible"] = (
            self.df["is_budget_reported"] & self.df["is_revenue_reported"]
        )


# ===================================================================
# RatingProcessor
# ===================================================================

class RatingProcessor:
    """Flattens and cleans the nested ratings JSON."""

    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path
        logger.info("RatingProcessor reading %s", file_path.name)

    def process(self) -> pd.DataFrame:
        """Parse JSON, flatten, and return a clean DataFrame."""
        try:
            with self._file_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            logger.error("Failed to read ratings file: %s", exc)
            raise

        rows = []
        for record in payload:
            summary = record.get("ratings_summary") or {}
            last_rated_ts = record.get("last_rated")
            rows.append(
                {
                    "movie_id": record.get("movie_id"),
                    "avg_rating": summary.get("avg_rating"),
                    "total_ratings": summary.get("total_ratings"),
                    "std_dev": summary.get("std_dev"),
                    "last_rated": (
                        datetime.fromtimestamp(last_rated_ts, tz=timezone.utc)
                        if last_rated_ts
                        else None
                    ),
                }
            )

        df = pd.DataFrame(rows)
        df["movie_id"] = pd.to_numeric(df["movie_id"], errors="coerce").astype("Int64")
        logger.info("RatingProcessor produced %d rows.", len(df))
        return df


# ===================================================================
# ExtendedMovieProcessor
# ===================================================================

class ExtendedMovieProcessor:
    """Cleans the movie_extended dataset (genres, companies, countries)."""

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()
        logger.info("ExtendedMovieProcessor initialised with %d rows.", len(self.df))

    def process(self) -> pd.DataFrame:
        """Run cleaning pipeline and return cleaned DataFrame."""
        self._deduplicate()
        self._cast_id()
        self._clean_production_countries()
        self._clean_spoken_languages()
        logger.info("ExtendedMovieProcessor finished: %d rows.", len(self.df))
        return self.df

    def _deduplicate(self) -> None:
        before = len(self.df)
        self.df = self.df.drop_duplicates(subset=["id"], keep="first")
        dropped = before - len(self.df)
        if dropped:
            logger.info("Dropped %d duplicate extended rows.", dropped)

    def _cast_id(self) -> None:
        self.df = normalize_id_column(self.df, "id")

    def _clean_production_countries(self) -> None:
        self.df["production_countries"] = self.df["production_countries"].apply(
            lambda x: parse_stringified_dict_list(x, "name")
        )

    def _clean_spoken_languages(self) -> None:
        self.df["spoken_languages"] = self.df["spoken_languages"].apply(
            lambda x: parse_stringified_dict_list(x, "name")
        )


# ===================================================================
# DB helpers
# ===================================================================

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def ensure_schema(engine, schema_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def write_table(engine, df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, con=engine, schema=SCHEMA, if_exists="replace", index=False)
    logger.info("Wrote %d rows → %s.%s", len(df), SCHEMA, table_name)


def run_csv_step(
    engine,
    file_name: str,
    table_name: str,
    processor_factory: Callable[[pd.DataFrame], object],
) -> None:
    """Read CSV, process via processor class, and write target table."""
    file_path = require_file(PROCESSED_DIR / file_name)
    raw_df = pd.read_csv(file_path)
    clean_df = processor_factory(raw_df).process()
    write_table(engine, clean_df, table_name)


# ===================================================================
# Main
# ===================================================================

def main() -> None:
    logger.info("=" * 60)
    logger.info("Pandas cleaning pipeline — bronze → silver_base")
    logger.info("=" * 60)

    engine = get_engine()
    ensure_schema(engine, SCHEMA)

    run_csv_step(engine, "movies_main.csv", "movies", MovieProcessor)

    ratings_path = require_file(PROCESSED_DIR / "ratings.json")
    ratings_clean = RatingProcessor(ratings_path).process()
    write_table(engine, ratings_clean, "ratings")

    run_csv_step(engine, "movie_extended.csv", "movie_extended", ExtendedMovieProcessor)

    logger.info("=" * 60)
    logger.info("Silver base ingestion complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
