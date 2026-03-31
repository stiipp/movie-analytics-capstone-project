"""
PySpark transformation pipeline that turns silver_base movie attributes into
gold_prep bridge-ready mappings.

Responsibilities:
  - Read cleaned silver_base extended movie attributes from Postgres
  - Explode multi-value genres, production companies, and production countries
  - Write gold_prep mapping tables used by downstream dbt models

Usage:
    python scripts/processing/transform_spark.py
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sqlalchemy import create_engine, inspect, text


def _running_in_docker() -> bool:
    # Detect container execution so local and Docker defaults can differ safely.
    return Path("/.dockerenv").exists()


SOURCE_SCHEMA = os.environ.get("SOURCE_SCHEMA", "silver_base")
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "gold_prep")

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
logger = logging.getLogger("transform_spark")


def jdbc_url() -> str:
    # JDBC connection string used by Spark for Postgres reads and writes.
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def db_url_sqlalchemy() -> str:
    # SQLAlchemy connection string used for schema checks and safe table writes.
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def create_spark() -> SparkSession:
    # Create the Spark session used for the mapping transformations.
    return (
        SparkSession.builder.appName("movie-analytics-transform-spark")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def ensure_target_schema() -> None:
    # Make sure the gold_prep schema exists before Spark starts writing tables.
    engine = create_engine(db_url_sqlalchemy())
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))


def _quote_ident(identifier: str) -> str:
    # Quote identifiers defensively for DDL statements built in Python.
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _spark_type_to_postgres(dtype: T.DataType) -> str:
    # Map Spark column types to Postgres types when syncing table schemas.
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


def _ensure_table_has_df_columns(engine, schema_name: str, table_name: str, df) -> None:
    # Add any newly introduced columns before writing so reruns do not fail on schema drift.
    inspector = inspect(engine)
    existing_cols = {
        col_info["name"].lower()
        for col_info in inspector.get_columns(table_name, schema=schema_name)
    }
    table_ident = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"

    added = 0
    with engine.begin() as conn:
        for field in df.schema.fields:
            if field.name.lower() in existing_cols:
                continue
            pg_type = _spark_type_to_postgres(field.dataType)
            col_ident = _quote_ident(field.name)
            conn.execute(
                text(
                    f"ALTER TABLE {table_ident} ADD COLUMN IF NOT EXISTS {col_ident} {pg_type}"
                )
            )
            existing_cols.add(field.name.lower())
            added += 1
            logger.info(
                "Added missing column %s.%s.%s (%s) to match Spark output schema.",
                schema_name,
                table_name,
                field.name,
                pg_type,
            )

    if added:
        logger.info(
            "Schema sync complete for %s.%s: %d column(s) added.",
            schema_name,
            table_name,
            added,
        )


def read_table(spark: SparkSession, table_name: str):
    # Read a cleaned silver_base table through JDBC so Spark can explode the multivalue fields.
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", f"{SOURCE_SCHEMA}.{table_name}")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_table(df, table_name: str) -> None:
    # Snapshot existing data before reload so a failed write can be restored safely.
    engine = create_engine(db_url_sqlalchemy())
    table_exists = inspect(engine).has_table(table_name, schema=TARGET_SCHEMA)
    qualified = f'{_quote_ident(TARGET_SCHEMA)}.{_quote_ident(table_name)}'
    backup = f'{_quote_ident(TARGET_SCHEMA)}.{_quote_ident(f"_{table_name}_backup")}'

    if table_exists:
        # Keep the target table object intact so downstream references do not break on reruns.
        _ensure_table_has_df_columns(engine, TARGET_SCHEMA, table_name, df)
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {backup}"))
            conn.execute(text(f"CREATE TABLE {backup} AS SELECT * FROM {qualified}"))
            conn.execute(text(f"TRUNCATE TABLE {qualified}"))
        write_mode = "append"
        logger.info(
            "Snapshotted and truncated existing table %s.%s before Spark load.",
            TARGET_SCHEMA,
            table_name,
        )
    else:
        write_mode = "error"

    try:
        (
            df.write.mode(write_mode)
            .format("jdbc")
            .option("url", jdbc_url())
            .option("dbtable", f"{TARGET_SCHEMA}.{table_name}")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
    except Exception:
        if table_exists:
            logger.error(
                "Spark write failed for %s.%s, restoring from backup snapshot.",
                TARGET_SCHEMA,
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

    logger.info("Wrote %s.%s (%d rows)", TARGET_SCHEMA, table_name, df.count())


def build_mapping(extended, source_column: str, value_name: str):
    # Turn comma-delimited extended attributes into one row per movie/value pair.
    # This converts denormalized text fields into bridge-ready tables that dbt
    # can join cleanly in the marts layer.
    return (
        extended.select(
            F.col("id").alias("movie_id"),
            F.explode_outer(
                F.split(F.coalesce(F.col(source_column), F.lit("")), ",")
            ).alias(value_name),
        )
        .withColumn(value_name, F.trim(F.col(value_name)))
        .filter(F.col("movie_id").isNotNull())
        .filter(F.col(value_name) != "")
        .dropDuplicates(["movie_id", value_name])
    )


def main() -> None:
    # Build and write all bridge-ready mapping tables for downstream dbt models.
    logger.info(
        "Starting PySpark transformation pipeline (%s -> %s)",
        SOURCE_SCHEMA,
        TARGET_SCHEMA,
    )

    ensure_target_schema()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # All mapping outputs are derived from the cleaned extended-attribute table.
    # Spark handles the repeated explode/trim/deduplicate pattern once here so
    # dbt receives clean bridge inputs instead of raw comma-delimited strings.
    extended = read_table(spark, "movie_extended")
    movie_genres = build_mapping(extended, "genres", "genre")
    movie_companies = build_mapping(extended, "production_companies", "company")
    movie_countries = build_mapping(extended, "production_countries", "country")

    # Write the bridge-ready mapping tables consumed by dbt staging models.
    write_table(movie_genres, "movie_genres")
    write_table(movie_companies, "movie_companies")
    write_table(movie_countries, "movie_countries")

    spark.stop()
    logger.info("PySpark transformation pipeline completed.")


if __name__ == "__main__":
    main()
