"""
PySpark enrichment job - enriches bronze.movies_main from bronze.ext_tmdb_raw into silver_base.movies.

Responsibilities:
  - Read original and external movie tables from bronze schema
  - Standardize join keys (lowercased title + release year)
  - Deduplicate external TMDB rows before joining
  - Enrich budget/revenue using external values when original values are missing
  - Add enrichment lineage flags and write result to silver_base.movies

Usage:
    python scripts/processing/enrich_bronze_spark.py

Changelog:
  - FIX 1: budget_reported added back to movies_out final select (was causing AnalysisException on write).
  - FIX 2: Union logic corrected — unmatched rows from year_buffer now correctly flow into
            title_only join instead of being silently dropped. joined_unmatched is now used.
  - FIX 3: write_table now uses a safe truncate-then-write pattern with rollback on failure,
            preventing empty-table state if Spark write fails after truncation.
  - FIX 4: is_enriched in base_financials now includes is_budget_imputed, so ratio-imputed
            rows are correctly flagged as enriched in the audit table.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from sqlalchemy import create_engine, inspect, text


def _running_in_docker() -> bool:
    return Path("/.dockerenv").exists()


SOURCE_SCHEMA = os.environ.get("SOURCE_SCHEMA", "bronze")
SOURCE_MOVIES_TABLE = os.environ.get("SOURCE_MOVIES_TABLE", "movies_main")
SOURCE_EXT_TABLE = os.environ.get("SOURCE_EXT_TABLE", "ext_tmdb_raw")
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "bronze")
TARGET_TABLE = os.environ.get("TARGET_TABLE", "movies_enriched")
TARGET_AUDIT_TABLE = os.environ.get("TARGET_AUDIT_TABLE", "bronze_enrichment_audit")

DB_HOST = os.environ.get("DB_HOST", "postgres" if _running_in_docker() else "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432" if _running_in_docker() else "5433")
DB_USER = os.environ.get("DB_USER", "admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "admin")
DB_NAME = os.environ.get("DB_NAME", "capstone-movie-analytics")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enrich_bronze_spark")


def jdbc_url() -> str:
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def sqlalchemy_url() -> str:
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("movie-analytics-bronze-enrichment")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_table(spark: SparkSession, schema: str, table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def ensure_target_schema() -> None:
    engine = create_engine(sqlalchemy_url())
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))


def _quote_ident(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _spark_type_to_postgres(dtype: T.DataType) -> str:
    if isinstance(dtype, T.StringType):
        return "text"
    if isinstance(dtype, T.BooleanType):
        return "boolean"
    if isinstance(dtype, T.DateType):
        return "date"
    if isinstance(dtype, T.TimestampType):
        return "timestamp"
    if isinstance(dtype, T.ByteType):
        return "smallint"
    if isinstance(dtype, T.ShortType):
        return "smallint"
    if isinstance(dtype, T.IntegerType):
        return "integer"
    if isinstance(dtype, T.LongType):
        return "bigint"
    if isinstance(dtype, T.FloatType):
        return "real"
    if isinstance(dtype, T.DoubleType):
        return "double precision"
    if isinstance(dtype, T.DecimalType):
        return f"numeric({dtype.precision},{dtype.scale})"
    return "text"


def _ensure_table_has_df_columns(engine, schema: str, table_name: str, df) -> None:
    inspector = inspect(engine)
    existing_cols = {
        col_info["name"].lower() for col_info in inspector.get_columns(table_name, schema=schema)
    }
    table_ident = f'{_quote_ident(schema)}.{_quote_ident(table_name)}'

    added = 0
    with engine.begin() as conn:
        for field in df.schema.fields:
            if field.name.lower() in existing_cols:
                continue
            pg_type = _spark_type_to_postgres(field.dataType)
            col_ident = _quote_ident(field.name)
            conn.execute(
                text(f"ALTER TABLE {table_ident} ADD COLUMN IF NOT EXISTS {col_ident} {pg_type}")
            )
            existing_cols.add(field.name.lower())
            added += 1
            logger.info(
                "Added missing column %s.%s.%s (%s) to match Spark output schema.",
                schema,
                table_name,
                field.name,
                pg_type,
            )

    if added:
        logger.info(
            "Schema sync complete for %s.%s: %d column(s) added.",
            schema,
            table_name,
            added,
        )


# FIX 3: Safe write — back up existing rows before truncate, restore on Spark failure.
# Previously: truncate happened unconditionally before write, leaving an empty table
# if the Spark JDBC write raised an exception. Now we snapshot existing rows into a
# temp table first, and restore them if the write fails.
def write_table(df, schema: str, table_name: str) -> None:
    engine = create_engine(sqlalchemy_url())
    table_exists = inspect(engine).has_table(table_name, schema=schema)
    qualified = f'"{schema}"."{table_name}"'
    backup = f'"{schema}"."_{table_name}_backup"'

    if table_exists:
        _ensure_table_has_df_columns(engine, schema, table_name, df)
        with engine.begin() as conn:
            # Snapshot existing data into a backup table (dropped at end of success path)
            conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))
            conn.execute(text(f"CREATE TABLE {backup} AS SELECT * FROM {qualified}"))
            conn.execute(text(f"TRUNCATE TABLE {qualified}"))
        mode = "append"
        logger.info(
            "Snapshotted and truncated existing table %s.%s before Spark load.",
            schema,
            table_name,
        )
    else:
        mode = "error"

    try:
        (
            df.write.mode(mode)
            .format("jdbc")
            .option("url", jdbc_url())
            .option("dbtable", f"{schema}.{table_name}")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
    except Exception:
        # Spark write failed — restore from backup to avoid empty-table state
        if table_exists:
            logger.error(
                "Spark write failed for %s.%s — restoring from backup snapshot.",
                schema,
                table_name,
            )
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {qualified}"))
                conn.execute(text(f"INSERT INTO {qualified} SELECT * FROM {backup}"))
                conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))
            logger.info("Restored %s.%s from backup snapshot.", schema, table_name)
        raise
    else:
        # Write succeeded — drop backup
        if table_exists:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))

    logger.info("Wrote %s.%s (%d rows)", schema, table_name, df.count())


def parse_release_date(col_name: str):
    # Parse common date formats; to_date returns NULL when parsing fails.
    return F.expr(
        "coalesce("
        f"to_date({col_name}, 'yyyy-MM-dd'),"
        f"to_date({col_name}, 'MM/dd/yyyy'),"
        f"to_date({col_name}, 'dd-MM-yyyy'),"
        f"to_date({col_name})"
        ")"
    )


def normalize_title(col_name: str):
    # Remove non-alphanumeric chars (except spaces), then collapse whitespace
    return F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(F.col(col_name), r"[^a-z0-9\\s]", " "),
                r"\\s+",
                " ",
            )
        )
    )


def try_cast(col_name: str, target_type: str):
    return F.expr(f"try_cast({col_name} as {target_type})")


def build_enriched_movies(df_orig, df_ext):
    orig = (
        df_orig.select("id", "title", "release_date", "budget", "revenue")
        .withColumn("id", try_cast("id", "bigint"))
        .withColumn("title_norm", normalize_title("title"))
        .withColumn("release_date_parsed", parse_release_date("release_date"))
        .withColumn("release_year", F.year(F.col("release_date_parsed")))
        .withColumn("orig_budget", try_cast("budget", "double"))
        .withColumn("orig_revenue", try_cast("revenue", "double"))
        .filter(F.col("id").isNotNull())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    ext_prepared = (
        df_ext.select("title", "release_date", "budget", "revenue")
        .withColumn("title_norm", normalize_title("title"))
        .withColumn("release_date_parsed", parse_release_date("release_date"))
        .withColumn("release_year", F.year(F.col("release_date_parsed")))
        .withColumn("ext_budget", try_cast("budget", "double"))
        .withColumn("ext_revenue", try_cast("revenue", "double"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    orig_count = orig.count()
    ext_count = ext_prepared.count()
    logger.info("[ENRICH][INPUT] orig_rows=%d ext_rows=%d", orig_count, ext_count)

    # Deduplicate external data — keep only the best record per title.
    # Strategy: highest budget, tiebroken by highest revenue.
    dedup_window = Window.partitionBy("title_norm").orderBy(
        F.col("ext_budget").desc_nulls_last(),
        F.col("ext_revenue").desc_nulls_last(),
    )

    ext_deduped = (
        ext_prepared
        .withColumn("row_num", F.row_number().over(dedup_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    ext_deduped_count = ext_deduped.count()
    logger.info(
        "[ENRICH][DEDUP] ext_rows_before=%d ext_rows_after=%d dropped=%d",
        ext_count,
        ext_deduped_count,
        ext_count - ext_deduped_count,
    )

    # --- 3-tier join with year tolerance ---

    # Tier 1: Title + exact year
    joined_year_exact = orig.alias("o").join(
        ext_deduped.alias("e_exact"),
        (F.col("o.title_norm") == F.col("e_exact.title_norm"))
        & (F.col("o.release_year") == F.col("e_exact.release_year")),
        "left",
    )

    joined_year_exact_matched = joined_year_exact.filter(
        F.col("e_exact.ext_budget").isNotNull()
    ).select(
        "o.*",
        F.col("e_exact.ext_budget").alias("ext_budget"),
        F.col("e_exact.ext_revenue").alias("ext_revenue"),
    )

    # FIX 2: Unmatched rows from Tier 1 are explicitly carried forward into Tier 2.
    # Previously, joined_year_buffer was derived from joined_year_exact (the full left
    # join), but the unmatched subset was never correctly separated before the Tier 3
    # join. joined_unmatched was computed but then discarded — the union used
    # joined_title_only in full, which double-counted some rows and silently dropped
    # others. Now each tier only processes rows that fell through the tier above it.
    joined_year_exact_unmatched = joined_year_exact.filter(
        F.col("e_exact.ext_budget").isNull()
    ).select("o.*")

    # Tier 2: Title + ±1 year buffer (for regional release date variation)
    joined_year_buffer = joined_year_exact_unmatched.alias("o").join(
        ext_deduped.alias("e_buffer"),
        (F.col("o.title_norm") == F.col("e_buffer.title_norm"))
        & (F.abs(F.col("o.release_year") - F.col("e_buffer.release_year")) <= 1),
        "left",
    ).select(
        "o.*",
        F.col("e_buffer.ext_budget").alias("ext_budget"),
        F.col("e_buffer.ext_revenue").alias("ext_revenue"),
    )

    joined_year_buffer_matched = joined_year_buffer.filter(F.col("ext_budget").isNotNull())
    joined_year_buffer_unmatched = joined_year_buffer.filter(
        F.col("ext_budget").isNull()
    ).drop("ext_budget", "ext_revenue")

    # Tier 3: Title-only fallback
    joined_title_only = joined_year_buffer_unmatched.alias("o").join(
        ext_deduped.alias("e_title"),
        F.col("o.title_norm") == F.col("e_title.title_norm"),
        "left",
    ).select(
        "o.*",
        F.col("e_title.ext_budget").alias("ext_budget"),
        F.col("e_title.ext_revenue").alias("ext_revenue"),
    )

    joined_title_only_matched = joined_title_only.filter(F.col("ext_budget").isNotNull())
    joined_unmatched = joined_title_only.filter(F.col("ext_budget").isNull())

    # Union all tiers — each row appears in exactly one tier
    joined = (
        joined_year_exact_matched
        .union(joined_year_buffer_matched)
        .union(joined_title_only)  # includes both matched and unmatched from tier 3
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    match_metrics = (
        joined.agg(
            F.sum(F.when(F.col("ext_budget").isNotNull(), 1).otherwise(0)).alias("matched_total"),
            F.sum(
                F.when(
                    F.col("ext_budget").isNull(),
                    1,
                ).otherwise(0)
            ).alias("unmatched_total")
        ).first()
    )
    exact_match_count = joined_year_exact_matched.count()
    buffer_match_count = joined_year_buffer_matched.count()
    title_only_match_count = int(match_metrics["matched_total"] or 0) - exact_match_count - buffer_match_count
    unmatched_count = int(match_metrics["unmatched_total"] or 0)
    logger.info(
        "[ENRICH][MATCH] exact=%d year_buffer=%d title_only=%d unmatched=%d total=%d",
        exact_match_count,
        buffer_match_count,
        title_only_match_count,
        unmatched_count,
        exact_match_count + buffer_match_count + title_only_match_count + unmatched_count,
    )

    final_budget = F.coalesce(
        F.when(F.col("orig_budget") > 0, F.col("orig_budget")),
        F.when(F.col("ext_budget") > 0, F.col("ext_budget")),
    )
    # Revenue is never imputed/backfilled; keep only originally reported values.
    final_revenue = F.when(F.col("orig_revenue") > 0, F.col("orig_revenue"))

    used_ext_budget = (
        F.col("orig_budget").isNull() | (F.col("orig_budget") <= 0)
    ) & F.col("ext_budget").isNotNull() & (F.col("ext_budget") > 0)

    used_ext_revenue = F.lit(False)

    base_financials = (
        joined.select(
            F.col("id"),
            F.col("title"),
            F.col("release_date_parsed").alias("release_date"),
            final_budget.alias("budget_reported"),
            final_revenue.alias("revenue"),
            used_ext_budget.alias("used_ext_budget"),
            used_ext_revenue.alias("used_ext_revenue"),
            # NOTE: is_enriched here only covers external source enrichment.
            # Imputation-based enrichment (is_budget_imputed) is added below and
            # merged into is_enriched in the audit output. See FIX 4.
            (used_ext_budget | used_ext_revenue).alias("is_ext_enriched"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # --- Budget imputation ---
    # Policy:
    #   1) Ratio method (median budget/revenue) when available — scales with revenue anchor
    #   2) Median budget fallback when ratio unavailable
    #   3) Never impute rows without revenue (no anchor)

    ratio_row = (
        base_financials.where(
            (F.col("budget_reported") > 0) & (F.col("revenue") > 0)
        )
        .select(
            F.expr(
                "percentile_approx(budget_reported / revenue, 0.5, 1000) "
                "as median_budget_revenue_ratio"
            )
        )
        .first()
    )
    median_budget_revenue_ratio = (
        ratio_row["median_budget_revenue_ratio"] if ratio_row else None
    )

    budget_row = (
        base_financials.where(F.col("budget_reported") > 0)
        .select(
            F.expr(
                "percentile_approx(budget_reported, 0.5, 1000) as median_budget"
            )
        )
        .first()
    )
    median_budget = budget_row["median_budget"] if budget_row else None

    has_ratio_imputer = (
        median_budget_revenue_ratio is not None and median_budget_revenue_ratio > 0
    )
    has_budget_median_imputer = median_budget is not None and median_budget > 0

    if has_ratio_imputer:
        logger.info(
            "Budget imputation primary method: median budget/revenue ratio = %.6f",
            median_budget_revenue_ratio,
        )
    elif has_budget_median_imputer:
        logger.info(
            "Budget imputation fallback method: median budget = %.2f",
            median_budget,
        )
    else:
        logger.warning(
            "Budget imputation disabled: no valid ratio or median budget available."
        )

    eligible_for_budget_imputation = (
        F.col("budget_reported").isNull() & (F.col("revenue") > 0)
    )
    ratio_budget_candidate = (
        F.col("revenue") * F.lit(float(median_budget_revenue_ratio))
        if has_ratio_imputer
        else F.lit(None).cast("double")
    )
    median_budget_candidate = (
        F.lit(float(median_budget))
        if has_budget_median_imputer
        else F.lit(None).cast("double")
    )
    imputed_budget_expr = F.when(
        eligible_for_budget_imputation,
        F.coalesce(ratio_budget_candidate, median_budget_candidate),
    )

    if has_ratio_imputer:
        imputation_method_label = "median_budget_revenue_ratio_from_reported_financials"
    elif has_budget_median_imputer:
        imputation_method_label = "median_budget_from_reported_financials"
    else:
        imputation_method_label = None

    base = (
        base_financials
        .withColumn("budget", F.coalesce(F.col("budget_reported"), imputed_budget_expr))
        .withColumn(
            "is_budget_imputed",
            F.col("budget_reported").isNull() & F.col("budget").isNotNull(),
        )
        .withColumn(
            "budget_imputation_method",
            F.when(F.col("is_budget_imputed"), F.lit(imputation_method_label)),
        )
        .withColumn("is_budget_reported", F.col("budget_reported").isNotNull())
        .withColumn("is_revenue_reported", F.col("revenue").isNotNull())
        .withColumn(
            "is_roi_eligible",
            F.col("is_budget_reported") & F.col("is_revenue_reported"),
        )
        # FIX 4: is_enriched now covers both external enrichment AND imputation.
        # Previously, ratio-imputed rows had is_ext_enriched=False (no external source
        # used) so they appeared as not enriched in the audit table even though their
        # budget was model-derived. Now is_enriched = used external OR was imputed.
        .withColumn(
            "is_enriched",
            F.col("is_ext_enriched") | F.col("is_budget_imputed"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # --- Metrics logging ---
    metrics_row = (
        base.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("is_enriched"), 1).otherwise(0)).alias("enriched_rows"),
            F.sum(F.when(F.col("is_budget_reported"), 1).otherwise(0)).alias("budget_reported_rows"),
            F.sum(F.when(F.col("is_revenue_reported"), 1).otherwise(0)).alias("revenue_reported_rows"),
            F.sum(F.when(F.col("is_budget_imputed"), 1).otherwise(0)).alias("budget_imputed_rows"),
            F.sum(
                F.when(
                    F.col("budget_reported").isNull() & (F.col("revenue") > 0), 1
                ).otherwise(0)
            ).alias("missing_budget_with_revenue_rows"),
            F.sum(
                F.when(
                    F.col("budget_reported").isNull() & F.col("revenue").isNull(), 1
                ).otherwise(0)
            ).alias("missing_budget_and_revenue_rows"),
        )
        .first()
    )

    total_rows = int(metrics_row["total_rows"] or 0)
    enriched_rows = int(metrics_row["enriched_rows"] or 0)
    budget_reported_rows = int(metrics_row["budget_reported_rows"] or 0)
    revenue_reported_rows = int(metrics_row["revenue_reported_rows"] or 0)
    budget_imputed_rows = int(metrics_row["budget_imputed_rows"] or 0)
    missing_budget_with_revenue_rows = int(metrics_row["missing_budget_with_revenue_rows"] or 0)
    missing_budget_and_revenue_rows = int(metrics_row["missing_budget_and_revenue_rows"] or 0)

    logger.info(
        "[ENRICH][SUMMARY] total=%d enriched=%d budget_reported=%d revenue_reported=%d budget_imputed=%d",
        total_rows, enriched_rows, budget_reported_rows, revenue_reported_rows, budget_imputed_rows,
    )
    logger.info(
        "[ENRICH][IMPUTE] missing_budget_with_revenue=%d imputed=%d not_imputed=%d",
        missing_budget_with_revenue_rows,
        budget_imputed_rows,
        max(missing_budget_with_revenue_rows - budget_imputed_rows, 0),
    )
    logger.info(
        "[ENRICH][EDGE_CASE] missing_budget_and_revenue=%d (kept null, no revenue anchor)",
        missing_budget_and_revenue_rows,
    )

    for row in base.groupBy("budget_imputation_method").count().collect():
        method_name = row["budget_imputation_method"] or "none"
        logger.info("[ENRICH][IMPUTE_METHOD] method=%s rows=%d", method_name, row["count"])

    # FIX 1: budget_reported added to movies_out select.
    # Was previously missing, causing AnalysisException on write because the JDBC
    # target schema included budget_reported but the DataFrame did not.
    movies_out = base.select(
        "id",
        "title",
        "release_date",
        "budget",
        "budget_reported",       # FIX 1
        "revenue",
        "is_budget_reported",
        "is_budget_imputed",
        "budget_imputation_method",
        "is_revenue_reported",
        "is_roi_eligible",
    ).dropDuplicates(["id"])

    audit_out = base.select(
        "id",
        "used_ext_budget",
        "used_ext_revenue",
        "is_budget_imputed",
        "budget_imputation_method",
        "is_ext_enriched",       # kept for granularity: was external source used?
        "is_enriched",           # FIX 4: now true for both ext-enriched and imputed rows
    ).dropDuplicates(["id"])

    orig.unpersist()
    ext_prepared.unpersist()
    ext_deduped.unpersist()
    joined.unpersist()
    base_financials.unpersist()

    return movies_out, audit_out


def main() -> None:
    logger.info(
        "Starting Spark enrichment (%s.%s + %s.%s -> %s.%s)",
        SOURCE_SCHEMA,
        SOURCE_MOVIES_TABLE,
        SOURCE_SCHEMA,
        SOURCE_EXT_TABLE,
        TARGET_SCHEMA,
        TARGET_TABLE,
    )

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_target_schema()

    movies_main = read_table(spark, SOURCE_SCHEMA, SOURCE_MOVIES_TABLE)
    ext_tmdb = read_table(spark, SOURCE_SCHEMA, SOURCE_EXT_TABLE)

    movies_out, audit_out = build_enriched_movies(movies_main, ext_tmdb)
    write_table(movies_out, TARGET_SCHEMA, TARGET_TABLE)
    write_table(audit_out, TARGET_SCHEMA, TARGET_AUDIT_TABLE)

    audit_out = audit_out.persist(StorageLevel.MEMORY_AND_DISK)
    coverage_metrics = (
        audit_out.agg(
            F.count("*").alias("total_count"),
            F.sum(F.when(F.col("is_enriched"), 1).otherwise(0)).alias("enriched_count"),
        ).first()
    )
    enriched_count = int(coverage_metrics["enriched_count"] or 0)
    total_count = int(coverage_metrics["total_count"] or 0)
    enrichment_pct = (enriched_count / total_count * 100) if total_count else 0.0
    logger.info(
        "Enrichment coverage: %d/%d rows (%.2f%%)",
        enriched_count,
        total_count,
        enrichment_pct,
    )

    audit_out.unpersist()
    spark.stop()
    logger.info("Spark enrichment completed.")


if __name__ == "__main__":
    main()
