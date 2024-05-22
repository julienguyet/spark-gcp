from jobs.tip_analysis.time import analyze as analyze_time
from jobs.tip_analysis.distance import analyze as analyze_distance
from jobs.tip_analysis.payment import analyze as analyze_payment

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    analyze_time(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    analyze_distance(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    analyze_payment(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    spark.stop()
