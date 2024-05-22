from jobs.demand_predictions.prediction import analyze as analyze_prediction

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    analyze_prediction(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    spark.stop()
