from pyspark.sql.functions import month, avg, unix_timestamp, dayofmonth, hour


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_enriched = df.withColumn(
        "duration_time_in_minutes",
        (
            unix_timestamp(df["tpep_dropoff_datetime"])
            - unix_timestamp(df["tpep_pickup_datetime"])
        )
        / 60,
    )

    # Performs basic analysis of dataset
    df_month = df_enriched.groupBy(
        month("tpep_pickup_datetime").alias("month")).agg(
        avg("duration_time_in_minutes").alias("average_trip_time_in_minutes")
    ).orderBy("month", ascending=True) \
    
    df_month.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/trip_analysis/month_analysis")

    df_day = df_enriched.groupBy(
        dayofmonth("tpep_pickup_datetime").alias("dayofmonth")).agg(
        avg("duration_time_in_minutes").alias("average_trip_time_in_minutes")
    ).orderBy("dayofmonth", ascending=True)

    df_day.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/trip_analysis/day_analysis")

    df_hour = df_enriched.groupBy(
        hour("tpep_pickup_datetime").alias("hour")).agg(
        avg("duration_time_in_minutes").alias("average_trip_time_in_minutes")
    ).orderBy("hour", ascending=True)

    df_hour.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/trip_analysis/hour_analysis")
