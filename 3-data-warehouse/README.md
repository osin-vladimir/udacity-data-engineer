## Project: Data Warehouse 


## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL
pipeline that extracts their data from S3, stages them in Redshift, and transforms data 
into a set of dimensional tables for their analytics team to continue finding insights in what songs
their users are listening to. You'll be able to test your database and ETL pipeline by running queries 
given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project description 
In this project, you will need to load data from S3 to staging 
tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

**tech stack**: aws redshift, aws s3, boto3, python, sql  

### Project structure
    
    3-data-warehouse/
    ├── create_tables.py          # creates tables predifined in sql_queries.py
    ├── crate_redshift_cluster.py # creates redshift cluster with access to S3
    ├── etl.py                    # general ETL script that using  whole dataset
    ├── sql_queries.py            # create, drop, insert, select queries for defined tables
    └── test.ipynb                # notebook that testing query results of etl.ipynb

To represent results you should run scripts in the following order:

1. Create a [cluster security group](https://docs.aws.amazon.com/redshift/latest/mgmt/manage-security-group-api-cli.html) 
in your VPC, that allow you to access your AWS Redshift cluster. 

2. Create Redshift cluster using command below:
    ```
    python create_redshift_cluster.py --aws-profile <put your AWS profile here>
    ```
   
   **Note**: keep in mind that your Redshift cluster should be in the same region as S3 buckets. 
   
3. Create required tables: 
    ```
    python create_tables.py
    ```
   
4. Execute ETL pipeline using:
    ```
    python etl.py
    ```
   
