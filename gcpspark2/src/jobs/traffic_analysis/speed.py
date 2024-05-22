from pyspark.sql.functions import col, hour, dayofweek, weekofyear, mean, unix_timestamp

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df = df.withColumn("trip_duration_hours", 
                (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600)
    df = df.withColumn("avg_speed_mph", col("trip_distance") / col("trip_duration_hours"))
    df = df.filter((col("trip_duration_hours") > 0) & (col("trip_distance") > 0))
    df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    df = df.withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime"))
    df = df.withColumn("pickup_weekofyear", weekofyear("tpep_pickup_datetime"))

    df_grouped_hour = df.groupBy("pickup_hour").agg(mean("avg_speed_mph").alias("avg_speed_mph_hour"))

    df_grouped_hour.repartition(1) \
            .write \
            .mode("overwrite") \
            .format("csv") \
            .option("header", "true") \
            .save(f"{gcs_output_path}/traffic_analysis/avg_speed_hourly")
    
    df_grouped_day = df.groupBy("pickup_dayofweek").agg(mean("avg_speed_mph").alias("avg_speed_mph_day"))

    df_grouped_day.repartition(1) \
            .write \
            .mode("overwrite") \
            .format("csv") \
            .option("header", "true") \
            .save(f"{gcs_output_path}/traffic_analysis/avg_speed_daily")
    
    df_grouped_week = df.groupBy("pickup_weekofyear").agg(mean("avg_speed_mph").alias("avg_speed_mph_week"))

    df_grouped_week.repartition(1) \
            .write \
            .mode("overwrite") \
            .format("csv") \
            .option("header", "true") \
            .save(f"{gcs_output_path}/traffic_analysis/avg_speed_weekly")
    