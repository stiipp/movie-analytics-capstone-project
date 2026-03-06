"""
PySpark transformation pipeline — transforms silver_base into gold_prep.

Responsibilities:
  - Read cleaned silver_base tables from Postgres
  - Join movies, ratings, and extended attributes
  - Explode multi-value dimensions (genres/companies/countries)
  - Build analytical aggregate tables for downstream marts

Usage:
    python scripts/processing/transform_spark.py
"""

import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _running_in_docker() -> bool:
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
    return f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


def db_url_sqlalchemy() -> str:
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def jdbc_properties() -> dict:
    return {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def create_spark() -> SparkSession:
    # Pull PostgreSQL JDBC driver so Spark can read/write via JDBC.
    return (
        SparkSession.builder.appName("movie-analytics-transform-spark")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def ensure_target_schema() -> None:
    engine = create_engine(db_url_sqlalchemy())
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))


def read_table(spark: SparkSession, table_name: str):
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
    (
        df.write.mode("overwrite")
        .format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", f"{TARGET_SCHEMA}.{table_name}")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .save()
    )
    logger.info("Wrote %s.%s (%d rows)", TARGET_SCHEMA, table_name, df.count())


def build_movie_enriched(movies, extended, ratings):
    movies = movies.select(
        F.col("id").alias("movie_id"),
        "title",
        F.to_date("release_date").alias("release_date"),
        "budget",
        "revenue",
        "is_budget_reported",
        "is_revenue_reported",
        "is_roi_eligible",
    )

    extended = extended.select(
        F.col("id").alias("movie_id"),
        "genres",
        "production_companies",
        "production_countries",
        "spoken_languages",
    )

    ratings = ratings.select(
        "movie_id",
        "avg_rating",
        "total_ratings",
        "std_dev",
        F.to_timestamp("last_rated").alias("last_rated"),
    )

    enriched = (
        movies.join(extended, on="movie_id", how="left")
        .join(ratings, on="movie_id", how="left")
        .withColumn("release_year", F.year("release_date"))
        .withColumn("release_month", F.date_trunc("month", F.col("release_date")))
        .withColumn(
            "profit",
            F.when(
                F.col("is_roi_eligible"),
                F.col("revenue") - F.col("budget"),
            ),
        )
        .withColumn(
            "roi",
            F.when(
                F.col("is_roi_eligible"),
                (F.col("revenue") - F.col("budget")) / F.col("budget"),
            ),
        )
    )

    # Performance requirement: cache post-join frame reused by multiple outputs.
    return enriched.persist(StorageLevel.MEMORY_AND_DISK)


def explode_dimension(df, source_col: str, output_col: str):
    return (
        df.select("movie_id", F.explode_outer(F.split(F.coalesce(F.col(source_col), F.lit("")), ",")).alias(output_col))
        .withColumn(output_col, F.trim(F.col(output_col)))
        .filter(F.col(output_col) != "")
    )


def main() -> None:
    logger.info("Starting PySpark transformation pipeline (%s -> %s)", SOURCE_SCHEMA, TARGET_SCHEMA)

    ensure_target_schema()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    movies = read_table(spark, "movies")
    extended = read_table(spark, "movie_extended")
    ratings = read_table(spark, "ratings")

    movie_enriched = build_movie_enriched(movies, extended, ratings)
    write_table(movie_enriched, "movie_enriched")

    genres = explode_dimension(movie_enriched, "genres", "genre")
    companies = explode_dimension(movie_enriched, "production_companies", "company")
    countries = explode_dimension(movie_enriched, "production_countries", "country")

    write_table(genres, "movie_genres")
    write_table(companies, "movie_companies")
    write_table(countries, "movie_countries")

    genre_metrics = (
        movie_enriched.join(genres, on="movie_id", how="left")
        .groupBy("release_year", "genre")
        .agg(
            F.countDistinct("movie_id").alias("movie_count"),
            F.avg("revenue").alias("avg_revenue"),
            F.avg("budget").alias("avg_budget"),
            F.avg("roi").alias("avg_roi"),
            F.avg("avg_rating").alias("avg_rating"),
        )
        .filter(F.col("genre").isNotNull())
    )

    monthly_metrics = (
        movie_enriched.groupBy("release_month")
        .agg(
            F.countDistinct("movie_id").alias("movie_count"),
            F.sum("revenue").alias("total_revenue"),
            F.sum("budget").alias("total_budget"),
            F.avg("roi").alias("avg_roi"),
        )
        .filter(F.col("release_month").isNotNull())
    )

    write_table(genre_metrics, "genre_year_metrics")
    write_table(monthly_metrics, "monthly_metrics")

    movie_enriched.unpersist()
    spark.stop()
    logger.info("PySpark transformation pipeline completed.")


if __name__ == "__main__":
    main()
