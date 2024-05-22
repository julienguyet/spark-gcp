# Deploy a Spark cluster on GCP 
<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/googlecloud/googlecloud-original-wordmark.svg" title="GCP" alt="GCP" width="200" height="200"/>&nbsp;
   <img src="https://github.com/devicons/devicon/blob/master/icons/apachespark/apachespark-original-wordmark.svg" title="Spark" alt="Spark" width="200" height="200"/>&nbsp;
</div>

---

## 0. Project Architecture

------------

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

## 1. Introduction

In this tutorial we will see how we can deploy a Spark cluster on Google Cloud and how to use it to:
- Process large amount of data and extract insghts from this data :dna:
- Perform some Machine Learning Prediction job :robot:

Start by cloning this repository and once process is finished, please move to the next section. If you already have the GCP CLI set up and your own bucket and cluster define, you can jump to section 5.

---

## 2. Set up your GCP account

First thing to do is for you to create a [Google Cloud Platform](https://cloud.google.com/?hl=en) account if you do not have one. As a new user you will enjoy some free credits (even if being asked for your credit card) and those credits will be more than enough to cover what we will perform here. Just don't forget to stop the cluser (and therefore close the VM) when you are done.

Once your account is created you can go to the console [here](https://console.cloud.google.com/welcome). Feel free to explore different options presented to you if you're interested. 
Then, in the search bar and search for "billing" account and make sure to link a valid billing account to your project. When you are fully set up, move to the below steps. 

---

# 3. Create the GCP bucket

Follow the instructions in the official [doc](https://cloud.google.com/sdk/docs/install) corresponding to your operating system to install gcloud & gsutil. You might need to replace the google-cloud-dsk in the current folder. Once process is finished, you can run the below commands to check installation was successful:

```bash
gcloud --help 
# Then run 
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

# 4. Create your cluster

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

## 5. Run a job

Image for make submit_job:

<img width="536" alt="Screenshot 2024-05-20 at 15 41 54" src="https://github.com/julienguyet/spark-gcp/assets/55974674/071b3a31-ed1d-4cf4-82dc-9f3b0a8b7a9c">

Image for job running:

<img width="1048" alt="Screenshot 2024-05-21 at 13 17 46" src="https://github.com/julienguyet/spark-gcp/assets/55974674/813ffff6-c38e-49df-841e-4daf3ed1ce92">

Image for Bucket:

<img width="356" alt="Screenshot 2024-05-22 at 18 42 04" src="https://github.com/julienguyet/spark-gcp/assets/55974674/88bc3ad5-f386-4af0-9e5e-02b6d9d66ae1">
