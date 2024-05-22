from pyspark.sql.functions import month, hour, avg,dayofweek,year

# Trip distance

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_hour = df.groupBy(hour("tpep_pickup_datetime").alias("hour")) \
        .agg(avg("tip_amount").alias("average_tip_amount")) \
        .orderBy("hour", ascending=[False, True])
    
    df_hour.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/hour_analysis")

    df_day = df.groupBy(dayofweek("tpep_pickup_datetime").alias("day")) \
        .agg(avg("tip_amount").alias("average_tip_amount")) \
        .orderBy(("day"), ascending = False)

    df_day.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/day_analysis")
    
    df_month = df.groupBy(month("tpep_pickup_datetime").alias("month")) \
    .agg(avg("tip_amount").alias("average_tip_amount")) \
    .orderBy(("month"), ascending = False)

    df_month.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/month_analysis")

    df_year = df.groupBy(year("tpep_pickup_datetime").alias("year")) \
    .agg(avg("tip_amount").alias("average_tip_amount")) \
    .orderBy(("year"), ascending = False) \
    .where("year < 2025 ")

    df_year.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/year_analysis")