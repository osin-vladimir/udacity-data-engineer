# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format.

From this tables we will be able to find insights in what songs their users are listening to.

## Project description

In this project, you need to build an ETL pipeline for a data lake hosted on S3. You will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. Deployment of this Spark process on a cluster using AWS EMR is part of the job.

**tech stack**: aws emr, aws s3, python, pyspark

## Project implementation

Fact and dimensional tables were defined and saved using Spark dataframes, which support necessary filtering options for selection of required data and
allow data merging via join operations. Intermediate and final results were saved in efficient **parquet** format.

### Project structure

    3-data-warehouse/
    ├── etl.py         # general ETL script to transform data using Spark
    ├── dl.cfg         # AWS keys, credentials


Follow the instruction below:

1. Populate AWS credentials in ``dl.cfg``.

2. Move your files from local environment to EMR:

    ```
    scp -i <your_pem_file.pem> </path/to/local/dir> <username>@<cluster-master-node-endpoint>:/path/emr
    ```

3. In order to run Spark job run the following command:

    ```
    spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 3 --executor-memory 4g --executor-core 2
    ```




