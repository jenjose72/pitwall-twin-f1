"""
Spark Structured Streaming job for feature engineering.
Ingests telemetry, performs windowing, joins with lap context.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, avg, stddev,
    lag, lead, when, lit, unix_timestamp, coalesce, to_json, struct, max, first
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# Define schemas
telemetry_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("driver", StringType()),
    StructField("speed", DoubleType()),
    StructField("rpm", DoubleType()),
    StructField("throttle", DoubleType()),
    StructField("brake", DoubleType()),
    StructField("gear", IntegerType()),
    StructField("drs", IntegerType()),
    StructField("x", DoubleType()),
    StructField("y", DoubleType()),
    StructField("z", DoubleType()),
    StructField("session_time", DoubleType()),
])

laps_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("driver", StringType()),
    StructField("lap_number", IntegerType()),
    StructField("tyre_compound", StringType()),
    StructField("tyre_age", IntegerType()),
    StructField("pit_in_time", TimestampType()),
    StructField("pit_out_time", TimestampType()),
    StructField("lap_time", DoubleType()),
])

weather_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("air_temp", DoubleType()),
    StructField("track_temp", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_direction", IntegerType()),
    StructField("wind_speed", DoubleType()),
    StructField("rainfall", IntegerType()),
])


def create_spark_session():
    return SparkSession \
        .builder \
        .appName("F1FeatureEngineering") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()


def feature_engineering():
    spark = create_spark_session()

    # Read telemetry from Kafka
    telemetry_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "f1.telemetry") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            from_json(col("value").cast("string"), telemetry_schema).alias("telemetry")
        ) \
        .select("telemetry.*") \
        .withColumn("event_time", col("timestamp")) \
        .withWatermark("event_time", "10 seconds")

    # Read laps context
    laps_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "f1.laps") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            from_json(col("value").cast("string"), laps_schema).alias("lap")
        ) \
        .select("lap.*") \
        .select(
            col("driver"),
            col("lap_number"),
            col("tyre_compound"),
            col("tyre_age"),
            col("timestamp").alias("lap_timestamp")
        )

    # Read weather
    weather_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "f1.weather") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            from_json(col("value").cast("string"), weather_schema).alias("weather")
        ) \
        .select("weather.*") \
        .select(
            col("timestamp").alias("weather_timestamp"),
            col("air_temp"),
            col("track_temp"),
            col("humidity"),
            col("pressure"),
            col("rainfall")
        ) \
        .withWatermark("weather_timestamp", "30 seconds")

    # Join telemetry with laps (latest lap context per driver)
    telemetry_laps = telemetry_df.join(
        laps_df,
        on=[
            (telemetry_df.driver == laps_df.driver) &
            (telemetry_df.timestamp >= laps_df.lap_timestamp)
        ],
        how="left_outer"
    )

    # Join with weather (nearest weather record)
    telemetry_context = telemetry_laps.join(
        weather_df,
        on=(telemetry_laps.timestamp >= weather_df.weather_timestamp),
        how="left_outer"
    ).drop("weather_timestamp")

    # Feature engineering: compute rolling statistics per driver
    features = telemetry_context \
        .groupBy(
            window(col("timestamp"), "10 seconds", "1 second"),
            col("driver")
        ) \
        .agg(
            avg("speed").alias("speed_mean"),
            stddev("speed").alias("speed_std"),
            avg("rpm").alias("rpm_mean"),
            stddev("rpm").alias("rpm_std"),
            avg("throttle").alias("throttle_mean"),
            avg("brake").alias("brake_mean"),
            avg("track_temp").alias("track_temp_mean"),
            max("lap_number").alias("current_lap"),
            first("tyre_compound").alias("tyre_compound"),
            first("tyre_age").alias("tyre_age"),
            first("rainfall").alias("rainfall")
        ) \
        .select(
            col("window.start").alias("feature_time"),
            col("driver"),
            col("speed_mean"),
            col("speed_std"),
            col("rpm_mean"),
            col("rpm_std"),
            col("throttle_mean"),
            col("brake_mean"),
            col("track_temp_mean"),
            col("current_lap"),
            col("tyre_compound"),
            col("tyre_age"),
            col("rainfall")
        )

    # Write features to Kafka
    features_query = features \
        .select("*") \
        .withColumn("value", to_json(struct("*"))) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "f1.features") \
        .option("checkpointLocation", "/tmp/features_checkpoint") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    feature_engineering()
