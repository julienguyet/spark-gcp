from jobs.trip_analysis_bis.duration import analyze as analyze_duration
from jobs.trip_analysis_bis.distance import analyze as analyze_distance
from jobs.trip_analysis_bis.top_locations import analyze as analyze_location


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    analyze_duration(
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
    analyze_location(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    spark.stop()
