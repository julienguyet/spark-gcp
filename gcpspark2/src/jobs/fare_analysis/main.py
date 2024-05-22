from jobs.fare_analysis.distance import analyze as analyze_distance
from jobs.fare_analysis.location import analyze as analyze_location
from jobs.fare_analysis.passenger import analyze as analyze_passenger


def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    print(f"Main Input path: {gcs_input_path}")
    print(f"Main Output path: {gcs_output_path}")
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
    analyze_passenger(
        spark,
        format=format,
        gcs_input_path=gcs_input_path,
        gcs_output_path=gcs_output_path,
    )
    spark.stop()
