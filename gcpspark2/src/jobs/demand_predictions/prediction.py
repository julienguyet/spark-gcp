from pyspark.sql.functions import col, hour, dayofweek, count, year, month, dayofmonth, lag, floor
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

def analyze(spark, format="parquet", gcs_input_path=None, gcs_output_path=None):
    df = spark.read.format(format).load(gcs_input_path)

    # Create new features based on date field
    df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime"))
    df = df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
    df = df.withColumn("day_of_month", dayofmonth("tpep_pickup_datetime"))
    df = df.withColumn("month", month("tpep_pickup_datetime"))
    df = df.withColumn("year", year("tpep_pickup_datetime"))
    df_grouped = df.groupBy("tpep_pickup_datetime", "hour_of_day", "day_of_week", "day_of_month", "month", "year").agg(count("*").alias("num_pickups"))

    # Create lag features
    windowSpec = Window.orderBy("tpep_pickup_datetime")
    df_grouped = df_grouped.withColumn("lag_1", lag("num_pickups", 1).over(windowSpec))
    df_grouped = df_grouped.withColumn("lag_2", lag("num_pickups", 2).over(windowSpec))

    df_grouped = df_grouped.na.drop()

    feature_columns = ["hour_of_day", "day_of_week", "day_of_month", "month", "year"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    rf = RandomForestRegressor(featuresCol="features", labelCol="num_pickups", numTrees=100)
    pipeline = Pipeline(stages=[assembler, rf])

    # Train Test Split
    train_df = df_grouped.filter(col("year") < 2021)
    test_df = df_grouped.filter(col("year") >= 2021)

    # Train the model
    model = pipeline.fit(train_df)

    # Make predictions
    predictions = model.transform(test_df)
    df_results = predictions.select(col("prediction").cast("int").floor().alias("rounded_prediction"), "num_pickups")

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="num_pickups", predictionCol="prediction", metricName="mse")
    mse = evaluator.evaluate(predictions)
    print("Mean Squared Error (MSE) on test data =", mse)
    evaluator = evaluator.setMetricName("rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data =", rmse)

    # Save predictions (using 'features' if 'indexed_features' is missing)
    df_results.repartition(1) \
        .write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{gcs_output_path}/predictions/pick_up_demands")
