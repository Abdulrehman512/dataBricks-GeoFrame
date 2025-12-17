# Databricks notebook source
# ============================================================
# Geo IP Resolution Pipeline
# Author: Abdul Rehman
# Description:
#   Resolves IPv4 addresses to geographic locations using
#   MaxMind-style subnet matching. Optimized for Databricks.
# ============================================================

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.functions import broadcast

# ============================================================
# Runtime Parameters (Passed from Airflow)
# ============================================================

# For local testing (comment in Databricks)
# dbutils.widgets.text("run_timestamp", "2024-02-21T09:00:00+00:00")

run_timestamp = dbutils.widgets.get("run_timestamp")
run_timestamp_formatted = datetime.strptime(
    run_timestamp, "%Y-%m-%dT%H:%M:%S%z"
) - timedelta(hours=1)

run_date = str(run_timestamp_formatted.date())
previous_hour = run_timestamp_formatted.strftime("%H")

# ============================================================
# IPv4 Utility
# ============================================================

def ipv4_to_long(ip_col):
    """
    Convert IPv4 dotted string to 32-bit integer
    """
    parts = F.split(ip_col, "\\.")
    return (
        parts.getItem(0).cast(LongType()) * 16777216 +
        parts.getItem(1).cast(LongType()) * 65536 +
        parts.getItem(2).cast(LongType()) * 256 +
        parts.getItem(3).cast(LongType())
    )

# ============================================================
# Data Readers
# ============================================================

def get_latest_geo_version(spark: SparkSession) -> int:
    return (
        spark.read.parquet(
            "s3://<your-bucket-name>/<your-path>geo"
        )
        .where("active")
        .select("geo_version")
        .first()[0]
    )

def read_network_locations(spark: SparkSession, geo_version: int) -> DataFrame:
    return (
        spark.read.parquet(
            f"s3://<your-bucket-name>/<your-path>"
        )
        .select("network", "postal_code", "geoname_id")
        .withColumns({
            "network_host": ipv4_to_long(F.split("network", "/")[0]),
            "mask_bytes": F.split("network", "/")[1].cast("int"),
            "geo_version": F.lit(geo_version)
        })
    )

def read_locdata(spark: SparkSession, geo_version: int) -> DataFrame:
    return (
        spark.read.parquet(
            f"s3://<your-bucket-name>/<your-path>"
        )
        .select(
            "location",
            "geoname_id",
            "location_type_id",
            "hierarchy",
            "country_iso_code",
            "iso_code"
        )
        .withColumn(
            "location_ids",
            F.split(F.regexp_replace("hierarchy", r"\[|\]", ""), ",")
        )
    )

# ============================================================
# Geo Dimension Builder
# ============================================================

def build_geo(spark: SparkSession, geo_version: int) -> DataFrame:
    netloc = read_network_locations(spark, geo_version)
    locdata = read_locdata(spark, geo_version)

    postal_df = (
        locdata
        .filter(F.col("location_type_id") == 7)
        .select(
            F.col("location").alias("postal_code"),
            "geoname_id",
            "location_ids"
        )
    )

    state_df = (
        locdata
        .filter(
            (F.col("location_type_id") == 5) &
            (F.col("country_iso_code") == "US")
        )
        .select(
            F.col("location").alias("region"),
            "iso_code",
            F.concat(
                F.col("location"), F.lit(", United States")
            ).alias("full_region")
        )
    )

    return (
        netloc
        .join(postal_df, ["postal_code", "geoname_id"], "inner")
        .join(state_df, ["region"], "left")
        .select(
            "network_host",
            "mask_bytes",
            "location_ids",
            F.col("full_region").alias("name"),
            "iso_code"
        )
    )

# ============================================================
# IP Expansion & Masking
# ============================================================

def build_ips(spark: SparkSession, geo_df: DataFrame, guid_log_path: str) -> DataFrame:
    """
    Expand IPs to all subnet masks present in geo table
    """
    mask_list = [
        row.mask_bytes
        for row in geo_df.select("mask_bytes").distinct().collect()
    ]

    return (
        spark.read.parquet(guid_log_path)
        .select("ip", "advertiser_id")
        .distinct()
        .filter(F.col("ip").like("%.%.%.%"))
        .withColumn("ip_num", ipv4_to_long("ip"))
        .withColumn(
            "mask_bytes",
            F.explode(F.array([F.lit(m) for m in mask_list]))
        )
        .withColumn(
            "network_host",
            F.expr(
                "ip_num & CAST(pow(2, 32) - pow(2, 32 - mask_bytes) AS BIGINT)"
            )
        )
        .select("ip", "advertiser_id", "mask_bytes", "network_host")
    )

# ============================================================
# Main Pipeline
# ============================================================

spark = SparkSession.builder.getOrCreate()

guid_log_path = (
    f"s3://<your-bucket-name>/<your-path>"
)

latest_geo_version = get_latest_geo_version(spark)

geo_df = build_geo(spark, latest_geo_version)

guid_logs_df = build_ips(spark, geo_df, guid_log_path)

ips_with_location = (
    guid_logs_df
    .join(broadcast(geo_df), ["mask_bytes", "network_host"], "inner")
    .select(
        "ip",
        "advertiser_id",
        "location_ids",
        "name",
        "iso_code"
    )
    .withColumn(
        "location_ids_string",
        F.concat_ws(",", "location_ids")
    )
)

ips_without_location = (
    guid_logs_df
    .join(
        ips_with_location,
        ["ip", "advertiser_id"],
        "leftanti"
    )
    .select(
        "ip",
        "advertiser_id",
        F.lit(None).alias("location_ids"),
        F.lit(None).alias("name"),
        F.lit(None).alias("iso_code"),
        F.lit(None).alias("location_ids_string")
    )
)

final_df = ips_with_location.unionByName(ips_without_location)

# ============================================================
# Write Output
# ============================================================

(
    final_df
    .distinct()
    .repartition(200)
    .write
    .mode("overwrite")
    .parquet(
        f"s3://<your-bucket-name>/<your-path>"
    )
)
