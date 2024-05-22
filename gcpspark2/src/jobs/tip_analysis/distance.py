from pyspark.sql.functions import avg, col
BUCKET_NAME="pyspark-tutorial"

# Trip distance

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df = df.withColumn("tip_percentage", col("tip_amount") / col("total_amount"))

    df_avg_tip = df.groupBy("DOLocationID") \
        .agg(avg("tip_percentage").alias("average_tip_percentage")) \
        .orderBy("average_tip_percentage", ascending=False)

    df_avg_tip.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/tip_amount_drop")

    df_avg_tip = df.groupBy("PULocationID") \
        .agg(avg("tip_percentage").alias("average_tip_percentage")) \
        .orderBy("average_tip_percentage", ascending=False)

    df_avg_tip.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/tip_amount_pickup")

    df_distance = df.groupBy("trip_distance") \
        .agg(avg("tip_percentage").alias("average_tip_percentage")) \
        .orderBy("average_tip_percentage", ascending=False) \

    df_distance.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/tip_amount_distance")