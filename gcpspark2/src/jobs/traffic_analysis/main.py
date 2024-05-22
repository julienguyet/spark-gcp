from jobs.traffic_analysis.speed import analyze as analyze_speed

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    analyze_speed(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    spark.stop()
