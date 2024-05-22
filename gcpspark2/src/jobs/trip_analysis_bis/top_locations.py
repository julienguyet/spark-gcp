from pyspark.sql.functions import count

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)
    
    df_pickups = df.groupBy("PULocationID") \
    .agg(count("*").alias("drop_count")) \
    .orderBy(("drop_count"), ascending = False) \
    .limit(5)

    df_pickups.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/trip_analysis/top_pickup")

    df_drops = df.groupBy("DOLocationID") \
    .agg(count("*").alias("drop_count")) \
    .orderBy(("drop_count"), ascending = False) \
    .limit(5)

    df_drops.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/trip_analysis/top_drops")