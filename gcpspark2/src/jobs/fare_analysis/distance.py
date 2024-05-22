from pyspark.sql.functions import avg

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_distance = df.groupBy("trip_distance") \
        .agg(avg("fare_amount").alias("average_fare_amount")) \
        .orderBy(("average_fare_amount"), ascending = False)

    df_distance.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/fare/distance")