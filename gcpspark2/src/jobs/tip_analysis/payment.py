from pyspark.sql.functions import avg

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    df_payment = df.groupBy("payment_type") \
        .agg(avg("tip_amount").alias("average_tip")) \
        .orderBy("average_tip", ascending=False) \
    
    df_payment.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/tip/payment_type")