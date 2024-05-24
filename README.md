# Deploy a Spark cluster on GCP 
<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/googlecloud/googlecloud-original-wordmark.svg" title="GCP" alt="GCP" width="200" height="200"/>&nbsp;
   <img src="https://github.com/devicons/devicon/blob/master/icons/apachespark/apachespark-original-wordmark.svg" title="Spark" alt="Spark" width="200" height="200"/>&nbsp;
</div>

---

## 1. Introduction :open_book:

In this tutorial we will see how we can deploy a Spark cluster on Google Cloud and how to use it to:
- Process large amount of data and extract insghts from this data :dna:
- Perform some Machine Learning Prediction job :robot:

Start by cloning this repository and once process is finished, please move to the next section. If you already have the GCP CLI set up and your own bucket and cluster define, you can jump to section 5.

---

## 2. Set up your GCP account :cloud:

First thing to do is for you to create a [Google Cloud Platform](https://cloud.google.com/?hl=en) account if you do not have one. As a new user you will enjoy some free credits (even if being asked for your credit card) and those credits will be more than enough to cover what we will perform here. Just don't forget to stop the cluser (and therefore close the VM) when you are done.

Once your account is created you can go to the console [here](https://console.cloud.google.com/welcome). Feel free to explore different options presented to you if you're interested. 
Then, in the search bar and search for "billing" account and make sure to link a valid billing account to your project. When you are fully set up, move to the below steps. 

---

# 3. Create the GCP bucket :wastebasket:

The repository architecture looks like the below:

```
├── gcpspark2
    ├── Makefile           <- Makefile with commands to create GCP cluster, buckets and submit jobs
    │
    ├── spark_functions    <- A jupyter notebook to explore manually functions included in the different jobs
    │
    ├── assets             <- Screenshots of the GCP set up for reference
    │
    ├── data
        ├── NYC            <- Parquet files with data about taxi fares and trips in New York
    │
    ├── dist               <- Folder with py files to create the spark session in the cluster
    │
    ├── google-cloud-dsk   <- GCP components to be installed on your machine to use the GCP CLI
    │
    ├── src         <- Folder containing the python code
        ├── jobs            <- Jobs to be executed by the cluser
            │
            ├── demand_predictions   <- The prediction job to return predicted demand based on date
            ├── fare_analysis        <- Insights about fare based on distance, neighbors and time of day
            ├── tip_analysis         <- Analysis on how distance, payment type and time can influence tips
            ├── traffic_analysis     <- Impact of traffic and average speed on demand
            ├── trip_analysis_bis    <- General insights on rides (average time, distance, etc.)
```

Follow the instructions in the official [doc](https://cloud.google.com/sdk/docs/install) corresponding to your operating system to install gcloud & gsutil. You might need to replace the google-cloud-dsk in the current folder. Once process is finished, you can run the below commands to check installation was successful:

```bash
gcloud --help 
gsutil --help
```

```bash
gcloud init 
```

```bash
gcloud auth application-default login
```

```bash
gcloud auth login                    
```

Finally, create a GCP bucket - you can see this as your storage space (you can also do it from the web UI [here](https://console.cloud.google.com/storage/)).

```bash
gcloud storage buckets create gs://pyspark-tutorial-<replace-by-your-login>
```

Verify the creation of your bucket : 
```bash
gsutil ls
```

You should see:
```bash
gs://pyspark-tutorial-<replace-by-your-login>/
```

Now that your bucket has been created, you can save it the NYC dataset by executing the below code in your terminal:

```bash
gsutil cp data/NYC/* gs://pyspark-tutorial-<replace-by-your-login>/data/NYC
```

This might take some time based. Wait for the process to finish and then we will move to the final step. 

---

# 4. Create your cluster :desktop_computer:

In your IDE open the .env file, you will see this:

```
LOGIN=#your login
BUCKET_NAME=#your bucket name
CODE_VERSION=0.0.0
REGION=us-central1 # update based on your account configuration
ZONE=us-central1-c # update based on your account configuration
CLUSTER_NAME=nyc-cluster
JOB_NAME=fare_analysis # the job to be executed
```
Replace the LOGIN by your Google login (email address you provided when creating your account). Update the REGION and ZONE variables based on your GCP account set up. 
Finally, change the BUCKET_NAME by the name you gave to your bucket in step 3 when creating it. 

Then, execute the below:

```bash
make create_cluster
```

This might take some time. It is also possible for you to not see any output in your terminal. If so, go to the [Dataproc page](https://console.cloud.google.com/dataproc/clusters) in GCP to see the status of your cluster.

---

## 5. Run a job :zap:

Ok we are now fully set up. :nerd_face:

To submit a job, first start the cluster in the [GCP UI](https://console.cloud.google.com/dataproc/clusters). Once the cluster is running, at the root of the "gcpspark2" folder, run:

```bash
make submit_job
```

This will execute the job specified in the .env file defined in step 4. 

:warning: Be aware that the prediction job can run for a long time and we do not recommend to run it a multiple time to avoid fees :warning:

When a job is running, you should see something like this:


<img width="536" alt="Screenshot 2024-05-20 at 15 41 54" src="https://github.com/julienguyet/spark-gcp/assets/55974674/071b3a31-ed1d-4cf4-82dc-9f3b0a8b7a9c">

And in the GCP UI at cluster level:

<img width="1048" alt="Screenshot 2024-05-21 at 13 17 46" src="https://github.com/julienguyet/spark-gcp/assets/55974674/813ffff6-c38e-49df-841e-4daf3ed1ce92">

Once the job is finished, you will reveice a success statement in the terminal. Go to your [bucket](https://console.cloud.google.com/storage/) and check that results have been stored. You should see new folders in your bucket like on the following:

<img width="356" alt="Screenshot 2024-05-22 at 18 42 04" src="https://github.com/julienguyet/spark-gcp/assets/55974674/88bc3ad5-f386-4af0-9e5e-02b6d9d66ae1">

---

## 6. Results Analysis :telescope:

### 1. Trip Analysis :oncoming_taxi:

If we take a look at the average trip duration (in minutes) by month, we can see rides don't go above 17 minutes in average. Longest rides take place in the last quarter of the year. 
We can assume this is the busiest quarter of the year in New York, leading to more traffic and more travels as well.

```
+-----+----------------------------+
|month|average_trip_time_in_minutes|
+-----+----------------------------+
|    1|          13.961936996778018|
|    2|           14.88645331599335|
|    3|          14.730989737679693|
|    4|          15.510014296137554|
|    5|          15.906733275470684|
|    6|           16.82647779882308|
|    7|           16.62454203050517|
|    8|          16.572784095643392|
|    9|           17.50014227893857|
|   10|          17.420198691973553|
|   11|           17.83479951080372|
|   12|          17.450098898736474|
+-----+----------------------------+
```

At day level, we don't observe huge gaps between the average duration of the rides:
```
+----------+----------------------------+
|dayofmonth|average_trip_time_in_minutes|
+----------+----------------------------+
|         1|            16.8700317003618|
|         2|           16.63850357855743|
|         3|          16.814805932609275|
|         4|          16.634436068423742|
|         5|          16.471851994600172|
|         6|          16.303048600094233|
|         7|           16.89967505732017|
|         8|          16.748068462688696|
|         9|          16.866152009163915|
|        10|          16.966997906493383|
|        11|          16.356581592314033|
|        12|           16.65966323130634|
|        13|          16.420420252622005|
|        14|          16.530610045065306|
|        15|           16.60998932527697|
|        16|          16.516733337402467|
|        17|          16.755362833144687|
|        18|          16.453700421957517|
|        19|          16.286643447453454|
|        20|           16.50240728529657|
|        21|           16.45058722970549|
|        22|          16.551454231721493|
|        23|          16.905608238998095|
|        24|           16.85286041197579|
|        25|           16.33431842675341|
|        26|          16.267531516389788|
|        27|           16.34868048773006|
|        28|          16.504450346204877|
|        29|          16.571541060018863|
|        30|          16.776989224727362|
|        31|          15.851012375526803|
+----------+----------------------------+
```

Finally, if we go more granular and inspect hourly rides, longest ones are taking place between 2 and 6pm. This is probably related to either people coming back from work or customers traveling for business reasons.

```
+----+----------------------------+
|hour|average_trip_time_in_minutes|
+----+----------------------------+
|   0|           15.73026777401626|
|   1|          15.850162623066096|
|   2|          14.827951157970006|
|   3|          15.549170610945119|
|   4|           16.76969686715942|
|   5|          16.754208948062853|
|   6|           16.47000700343768|
|   7|          16.430668381762295|
|   8|           15.92411902626667|
|   9|          15.937159524944118|
|  10|          15.968707707679128|
|  11|          15.990086114978192|
|  12|            16.5502102865842|
|  13|          16.928557399414526|
|  14|           18.00871055109994|
|  15|          18.798311988582824|
|  16|          18.979807123045823|
|  17|           17.86783347900255|
|  18|           16.21771435626182|
|  19|          15.191404564765671|
|  20|          15.010945933820969|
|  21|          15.109525080809965|
|  22|          15.427000622889635|
|  23|          15.553266705082581|
+----+----------------------------+
```

Now regarding the average distance, we observe a few surprising insights: (i) longest rides are happening in the month of mai, (ii) average distance never goes above 14 miles execpt for rides between 4 and 6 am (see table below) and finally, the first and 24th of the month are the most busiest day on average. 

Some potential explanations are: (i) people travelling in the early morning or at night don't have any other alternative than the cab as public transports are closed, (ii) some days are overrepresented during holiday seasons (like the 1st of January and 24th of December). 

```
+----+------------------------------+
|hour|average_trip_distance_in_miles|
+----+------------------------------+
|   4|             45.89584296445569|
|   5|            43.514597392531904|
|   6|             28.82526404406175|
```

Finally, if we analyze the most popular locations for pickup and drop, some IDs are reccurent for both field:

```
+------------+------------+
|PULocationID|pickup_count|
+------------+------------+
|         237|     1553554|
|         236|     1424614|
|         161|     1091329|
|         132|     1025063|
|         186|     1019650|
+------------+------------+

+------------+----------+
|DOLocationID|drop_count|
+------------+----------+
|         236|   1434919|
|         237|   1356518|
|         161|   1001077|
|         170|    920433|
|         141|    902052|
+------------+----------+
```

As we can see, 237 - 236 - 161 are always in the top 5. These are probably IDs for districts with many offices or busy areas. To confirm this we could do some further analysis by adding the time of the day next to each pickup / drop ID.

### 2. Tip Analysis :dollar:

Looking at tips given by customers, we can see locations 255 and 249 are always in the top 5, counting as ~13% of the final bill.

```
+------------+----------------------+   
|DOLocationID|average_tip_percentage|
+------------+----------------------+
|          52|   0.13507571291857495|
|          40|    0.1349902504372998|
|         255|   0.13294293712786734|
|         138|   0.13097318414425485|
|         249|    0.1305343050804773|
+------------+----------------------+

+------------+----------------------+
|PULocationID|average_tip_percentage|
+------------+----------------------+
|         255|   0.13022838297171588|
|         249|   0.12950151760556006|
|         158|   0.12867509846430827|
|         199|   0.12681086193114427|
|         125|   0.12679905291940308|
+------------+----------------------+

```

Finally, we can see the tip is increasing with the trip distance:
```
+-------------+----------------------+
|trip_distance|average_tip_percentage|
+-------------+----------------------+
|        59.28|     0.683073832245103|
|     29707.75|    0.5571030640668524|
|      6206.38|    0.5141388174807198|
|        78.29|                   0.5|
|        427.7|   0.49748734950270457|
+-------------+----------------------+
```

When analysing at time level, tip amount is pretty flat at the hour level, always included between 2.1 and 2.9. Regarding the day of the week, tips are very alike with only a top on Mondays (2.48 on average). Finally, payment time has a major influence on the tip amount, as on average customers tend to tip more with payment type 1 and 0. However, we obtained some negative values for ID 2 and 3, which means some more analysis would be needed to identify outliers or totaly remove those payment types (based on business knowledge - maybe it's outdated types). 

```
+------------+--------------------+
|payment_type|         average_tip|
+------------+--------------------+
|           1|  3.0755510306720333|
|           0|  2.1700068168216124|
|           4|0.022958282745690756|
|           2|4.108590704647676E-4|
|           5|                 0.0|
|           3|-0.01167058060330...|
+------------+--------------------+
```

### 3. Fare Analysis :receipt:

Average fare amount is higher for some locations, for both drop and pickup. Especially IDs 44 - 84 - 204. These neighborhoods are maybe located far from the city center or located in some areas where fare rate is higher by default based on demand and time of the day. 

```
+------------+-------------------+
|PULocationID|average_fare_amount|
+------------+-------------------+
|          44|  99.34643231114435|
|          84|  90.55050847457628|
|         110|               84.5|
|         204|  83.64895833333334|
|          99|  82.19200000000001|
+------------+-------------------+

+------------+-------------------+
|DOLocationID|average_fare_amount|
+------------+-------------------+
|          44|  93.73268292682927|
|          84|  81.05751937984495|
|         204|  78.87132450331126|
|           5|  75.44233918128654|
|         265|  72.63122658480091|
+------------+-------------------+
```

As well, the fare rises with the number of passengers. That said, it is important to challenge those results as maybe people are more sharing cabs in context of business meetings or when going out in the evening, when fare base is higher.

```
+---------------+-------------------+
|passenger_count|average_fare_amount|
+---------------+-------------------+
|            9.0|              61.35|
|            7.0|  52.91679487179488|
|            8.0|  49.14408163265307|
|            4.0| 14.284687986716266|
|            2.0|  13.77639929394148|
|            3.0| 13.555663818461737|
|            6.0| 12.751094437062962|
|            1.0| 12.709557736571133|
|            5.0| 12.666400646383593|
+---------------+-------------------+
```
Finally, we also observed that the average fare amount is increasing with the distance, but the base of this fare amount (cost per miles) is highly decreasing if we compare top 5 longest distances with top 5 smallest rides:

```
+-------------+-------------------------+
|trip_distance|average_fare_amount_miles|
+-------------+-------------------------+
|       964.27|       2.5024111504039324|
|       821.54|       2.5026170362976847|
|       709.88|        1.714374260438384|
|        633.8|       1.6172294099084885|
|       448.47|       0.8919214217227461|
+-------------+-------------------------+

+-------------+-------------------------+
|trip_distance|average_fare_amount_miles|
+-------------+-------------------------+
|          1.0|        6.599282608601806|
|         1.01|       6.5126589046862655|
|         1.02|        6.484397563459084|
|         1.03|        6.465406383928224|
|         1.04|         6.44160974897299|
+-------------+-------------------------+
```

### 4. Traffic Analysis :red_car:



### 5. Demand Predictions :crystal_ball:

The idea is to predict the number of rides requested for a given date. To do this, we created new features in our dataset:

```
df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime"))
df = df.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
df = df.withColumn("day_of_month", dayofmonth("tpep_pickup_datetime"))
df = df.withColumn("month", month("tpep_pickup_datetime"))
df = df.withColumn("year", year("tpep_pickup_datetime"))
df_grouped = df.groupBy("hour_of_day", "day_of_week", "day_of_month", "month", "year").agg(count("*").alias("num_pickups"))
```

At first we used a "simple" Linear Regression model using ```from pyspark.ml.regression import LinearRegression```. However, he didn't get very convincing results:

<img width="495" alt="Screenshot 2024-05-24 at 14 25 15" src="https://github.com/julienguyet/spark-gcp/assets/55974674/f1d71bbe-2d9f-49d2-a8e7-f9b4dfd3e64b">

We observe a very high MSE but even worst, negative predictions - which, obviously, is impossible:

```                                                                               
+------------------+-----------+--------------------+
|        prediction|num_pickups|            features|
+------------------+-----------+--------------------+
| 2381.874002071474|       3766|[0.0,1.0,1.0,8.0,...|
|1499.0227822664137|       2993|[0.0,1.0,2.0,5.0,...|
| 851.7960312795994|        889|[0.0,1.0,7.0,2.0,...|
|  897.068399076058|       1130|[0.0,1.0,7.0,3.0,...|
|2409.9077245241083|       3889|[0.0,1.0,15.0,8.0...|
+------------------+-----------+--------------------+
only showing top 5 rows

                                                                                
Mean Squared Error (MSE) on test data = 965820.2792360616
Root Mean Squared Error (RMSE) on test data = 982.7615576710668
```

To improve our code, we took two steps: (i) update the model and implement Random Forest, and (ii) generate lag features.

We generated the lag features using the below functions:

```
windowSpec = Window.orderBy("tpep_pickup_datetime")
df_grouped = df_grouped.withColumn("lag_1", lag("num_pickups", 1).over(windowSpec))
df_grouped = df_grouped.withColumn("lag_2", lag("num_pickups", 2).over(windowSpec))
```

This allows us to create new data, including information from the previous rows. For the first column, we are looking one row back, when we take two steps back for the second column. This outputs the following:

```
+-------------------+-----------+-----+-----+
|tpep_pickup_datetime|num_pickups|lag_1|lag_2|
+-------------------+-----------+-----+-----+
|2024-05-20 08:00:00|         10| null| null|
|2024-05-20 09:00:00|         15|   10| null|
|2024-05-20 10:00:00|         12|   15|   10|
|2024-05-20 11:00:00|         20|   12|   15|
+-------------------+-----------+-----+-----+
```

By doing this, we are generating new features to feed the algorithm and potentially improve our predictions:

```
+-----------+-----------+------------+-----+----+-----------+----------+
|hour_of_day|day_of_week|day_of_month|month|year|num_pickups|Prediction|
+-----------+-----------+------------+-----+----+-----------+----------+
|          0|          6|           1|    1|2021|          1|       1.0|
|          0|          6|           1|    1|2021|          1|       1.0|
|          0|          6|           1|    1|2021|          1|       1.0|
|          0|          6|           1|    1|2021|          1|       1.0|
|          0|          6|           1|    1|2021|          1|       1.0|
+-----------+-----------+------------+-----+----+-----------+----------+
only showing top 5 rows

Mean Squared Error (MSE) on test data = 1.75
Root Mean Squared Error (RMSE) on test data = 1.32
```
