from pyspark.sql.functions import avg

# Trip distance

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):

    df = spark.read.format(format).load(gcs_input_path)

    df_passenger = df.groupBy("passenger_count") \
    .agg(avg("fare_amount").alias("average_fare_amount")) \
    .orderBy(("average_fare_amount"), ascending = False) \
    .where("passenger_count is not null")

    df_passenger.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/fare/passenger")