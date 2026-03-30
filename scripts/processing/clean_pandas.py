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

from __future__ import annotations

import json
import logging
import os
import sys
import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from sqlalchemy import create_engine, inspect, text

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


def log_count_check(dataset: str, metric: str, observed: int, expected: int = 0) -> None:
    """Log a small PASS/WARN quality check for easy console scanning."""
    status = "PASS" if observed == expected else "WARN"
    logger.info(
        "[CHECK][%s] %s.%s -> observed=%d expected=%d",
        status,
        dataset,
        metric,
        observed,
        expected,
    )


def update_check_totals(observed: int, expected: int, totals: dict[str, int]) -> None:
    """Track simple pass/warn counts for end-of-step summary logging."""
    if observed == expected:
        totals["pass"] += 1
    else:
        totals["warn"] += 1

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

    # Final fallback keeps parser resilient if rare date patterns appear later.
    fallback_mask = parsed.isna() & raw.notna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(raw.loc[fallback_mask], errors="coerce")

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
        self._ensure_optional_financial_columns()
        self._add_flags()
        self._log_quality_checks()
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

    def _ensure_optional_financial_columns(self) -> None:
        """Backfill optional enrichment columns so downstream models can rely on stable schema."""
        if "budget_reported" not in self.df.columns:
            self.df["budget_reported"] = self.df["budget"]
        else:
            self.df["budget_reported"] = pd.to_numeric(self.df["budget_reported"], errors="coerce")
            self.df["budget_reported"] = self.df["budget_reported"].where(
                self.df["budget_reported"] > 0,
                other=None,
            )

        if "is_budget_imputed" not in self.df.columns:
            self.df["is_budget_imputed"] = False
        else:
            self.df["is_budget_imputed"] = self.df["is_budget_imputed"].fillna(False).astype(bool)

        if "budget_imputation_method" not in self.df.columns:
            self.df["budget_imputation_method"] = None

    def _add_flags(self) -> None:
        # Derive reported-budget provenance from the pre-imputation column when present.
        # Using final budget would incorrectly mark imputed budgets as "reported".
        if "budget_reported" in self.df.columns:
            derived_budget_reported = self.df["budget_reported"].notna()
        else:
            derived_budget_reported = self.df["budget"].notna()
        derived_revenue_reported = self.df["revenue"].notna()

        if "is_budget_reported" in self.df.columns:
            self.df["is_budget_reported"] = self.df["is_budget_reported"].fillna(False).astype(bool)
        else:
            self.df["is_budget_reported"] = derived_budget_reported

        if "is_revenue_reported" in self.df.columns:
            self.df["is_revenue_reported"] = self.df["is_revenue_reported"].fillna(False).astype(bool)
        else:
            self.df["is_revenue_reported"] = derived_revenue_reported

        market_coverage_eligible = (
            self.df["is_revenue_reported"]
            & (self.df["budget"] >= 10000)
            & (self.df["revenue"] > 0)
        )
        recommendation_eligible = (
            self.df["is_budget_reported"]
            & self.df["is_revenue_reported"]
            & (self.df["budget"] >= 10000)
            & (self.df["revenue"] >= 1000000)
        )

        self.df["is_market_coverage_eligible"] = market_coverage_eligible
        self.df["is_recommendation_eligible"] = recommendation_eligible

        # Backward-compatibility aliases for legacy downstream references.
        self.df["roi_eligible_imputed"] = self.df["is_market_coverage_eligible"]
        self.df["roi_eligible_strict"] = self.df["is_recommendation_eligible"]
        self.df["is_roi_eligible"] = self.df["is_recommendation_eligible"]

    def _log_quality_checks(self) -> None:
        """Console-friendly quality checks for quick run validation."""
        totals = {"pass": 0, "warn": 0}

        id_nulls = int(self.df["id"].isna().sum())
        log_count_check("movies", "id_nulls", id_nulls, 0)
        update_check_totals(id_nulls, 0, totals)

        duplicate_ids = int(self.df.duplicated(subset=["id"]).sum())
        log_count_check("movies", "duplicate_ids", duplicate_ids, 0)
        update_check_totals(duplicate_ids, 0, totals)

        release_date_nulls = int(self.df["release_date"].isna().sum())
        log_count_check("movies", "release_date_nulls", release_date_nulls, 0)
        update_check_totals(release_date_nulls, 0, totals)

        logger.info(
            "[CHECK][INFO] movies.recommendation_eligible_rows=%d",
            int(self.df["is_roi_eligible"].sum()),
        )
        logger.info(
            "[CHECK][INFO] movies.market_coverage_eligible_rows=%d",
            int(self.df["is_market_coverage_eligible"].sum()),
        )
        logger.info(
            "[CHECK][INFO] movies.recommendation_eligible_reported_rows=%d",
            int(self.df["is_recommendation_eligible"].sum()),
        )
        logger.info(
            "[CHECK][SUMMARY] movies -> pass=%d warn=%d",
            totals["pass"],
            totals["warn"],
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
        self._log_quality_checks(df)
        logger.info("RatingProcessor produced %d rows.", len(df))
        return df

    def _log_quality_checks(self, df: pd.DataFrame) -> None:
        """Console-friendly quality checks for ratings output."""
        totals = {"pass": 0, "warn": 0}

        movie_id_nulls = int(df["movie_id"].isna().sum())
        log_count_check("ratings", "movie_id_nulls", movie_id_nulls, 0)
        update_check_totals(movie_id_nulls, 0, totals)

        duplicate_movie_ids = int(df.duplicated(subset=["movie_id"]).sum())
        log_count_check("ratings", "duplicate_movie_ids", duplicate_movie_ids, 0)
        update_check_totals(duplicate_movie_ids, 0, totals)

        avg_rating_nulls = int(df["avg_rating"].isna().sum())
        log_count_check("ratings", "avg_rating_nulls", avg_rating_nulls, 0)
        update_check_totals(avg_rating_nulls, 0, totals)

        total_ratings_nulls = int(df["total_ratings"].isna().sum())
        log_count_check("ratings", "total_ratings_nulls", total_ratings_nulls, 0)
        update_check_totals(total_ratings_nulls, 0, totals)

        last_rated_nulls = int(df["last_rated"].isna().sum())
        log_count_check("ratings", "last_rated_nulls", last_rated_nulls, 0)
        update_check_totals(last_rated_nulls, 0, totals)

        logger.info(
            "[CHECK][SUMMARY] ratings -> pass=%d warn=%d",
            totals["pass"],
            totals["warn"],
        )


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
        self._log_quality_checks()
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

    def _log_quality_checks(self) -> None:
        """Console-friendly quality checks for extended movie output."""
        totals = {"pass": 0, "warn": 0}

        id_nulls = int(self.df["id"].isna().sum())
        log_count_check("movie_extended", "id_nulls", id_nulls, 0)
        update_check_totals(id_nulls, 0, totals)

        duplicate_ids = int(self.df.duplicated(subset=["id"]).sum())
        log_count_check("movie_extended", "duplicate_ids", duplicate_ids, 0)
        update_check_totals(duplicate_ids, 0, totals)

        logger.info(
            "[CHECK][SUMMARY] movie_extended -> pass=%d warn=%d",
            totals["pass"],
            totals["warn"],
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


def _quote_ident(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _pandas_dtype_to_postgres(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(dtype):
        return "double precision"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "timestamp"
    if pd.api.types.is_timedelta64_dtype(dtype):
        return "interval"
    return "text"


def _ensure_table_has_df_columns(engine, schema_name: str, table_name: str, df: pd.DataFrame) -> None:
    inspector = inspect(engine)
    existing_cols = {
        col["name"].lower() for col in inspector.get_columns(table_name, schema=schema_name)
    }
    table_ident = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"

    added = 0
    with engine.begin() as conn:
        for col_name in df.columns:
            if col_name.lower() in existing_cols:
                continue
            pg_type = _pandas_dtype_to_postgres(df[col_name].dtype)
            col_ident = _quote_ident(col_name)
            conn.execute(
                text(
                    f"ALTER TABLE {table_ident} ADD COLUMN IF NOT EXISTS {col_ident} {pg_type}"
                )
            )
            existing_cols.add(col_name.lower())
            added += 1
            logger.info(
                "Added missing column %s.%s.%s (%s) to match pandas output schema.",
                schema_name,
                table_name,
                col_name,
                pg_type,
            )

    if added:
        logger.info(
            "Schema sync complete for %s.%s: %d column(s) added.",
            schema_name,
            table_name,
            added,
        )


def write_table(engine, df: pd.DataFrame, table_name: str) -> None:
    """
    Write DataFrame to target table without dropping the table object.

    Why this matters:
    - `if_exists="replace"` issues DROP TABLE, which breaks when downstream views
      depend on the table (for example dbt staging views).
    - Truncating + appending keeps dependencies valid across reruns.
    """
    table_exists = inspect(engine).has_table(table_name, schema=SCHEMA)
    qualified = f'{_quote_ident(SCHEMA)}.{_quote_ident(table_name)}'
    backup = f'{_quote_ident(SCHEMA)}.{_quote_ident(f"_{table_name}_backup")}'

    if table_exists:
        _ensure_table_has_df_columns(engine, SCHEMA, table_name, df)
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))
            conn.execute(text(f"CREATE TABLE {backup} AS SELECT * FROM {qualified}"))
            conn.execute(text(f"TRUNCATE TABLE {qualified}"))
        write_mode = "append"
        logger.info(
            "Snapshotted and truncated existing table %s.%s before load.",
            SCHEMA,
            table_name,
        )
    else:
        write_mode = "fail"

    try:
        df.to_sql(table_name, con=engine, schema=SCHEMA, if_exists=write_mode, index=False)
    except Exception:
        if table_exists:
            logger.error(
                "Pandas write failed for %s.%s, restoring from backup snapshot.",
                SCHEMA,
                table_name,
            )
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {qualified}"))
                conn.execute(text(f"INSERT INTO {qualified} SELECT * FROM {backup}"))
                conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))
        raise
    else:
        if table_exists:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))

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


def run_table_step(
    engine,
    source_schema: str,
    source_table: str,
    table_name: str,
    processor_factory: Callable[[pd.DataFrame], object],
) -> None:
    """Read from Postgres table, process via processor class, and write target table."""
    raw_df = pd.read_sql_table(source_table, con=engine, schema=source_schema)
    logger.info("Read %d rows from %s.%s", len(raw_df), source_schema, source_table)
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
    
    # Check if enriched movies exist in bronze schema (from spark_enrich)
    inspector = inspect(engine)
    bronze_tables = {table.lower() for table in inspector.get_table_names(schema="bronze")}
    
    if "movies_enriched" in bronze_tables:
        logger.info("Found bronze.movies_enriched from enrichment pipeline")
        run_table_step(engine, "bronze", "movies_enriched", "movies", MovieProcessor)
    else:
        logger.info("bronze.movies_enriched not found; reading from CSV")
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
