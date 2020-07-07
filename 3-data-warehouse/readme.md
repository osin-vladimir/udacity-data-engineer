# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database
and want to move their processes and data onto the cloud. Their data resides in
S3, in a directory of JSON logs on user activity on the app, as well as a directory
with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL
pipeline that extracts their data from S3, stages them in Redshift, and transforms data
into a set of dimensional tables for their analytics team to continue finding insights in what songs
their users are listening to. You'll be able to test your database and ETL pipeline by running queries
given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project description

In this project, you will need to load data from S3 to staging
tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

**tech stack**: aws redshift, aws s3, boto3, python, sql

## Project implementation

Fact and dimensional tables were defined in a star schema and focused on analysing played songs, which is the core focus of the analytics team in Sparkify. Start schema is perfectly suited for this purposes since it's allowing to construct efficient queries with small amount of joins. ETL pipeline designed using **SQL**, that allows to copy data from JSON and perform necessary filtering prior to writing into the database, and **psycopg2** library which was used for executing SQL queries for Redshift cluster.

### Project structure

    3-data-warehouse/
    ├── create_tables.py            # creates tables predifined in sql_queries.py
    ├── create_redshift_cluster.py  # creates redshift cluster with access to S3
    ├── add_security_group.py       # add security group to default VPC in order to access Rdshift cluster
    ├── delete_redshift_cluster.py  # deletes redshift cluster and roles
    ├── etl.py                      # general ETL script that using  whole dataset
    ├── dwh.cfg                     # your Redshift cluster credentials, policies
    └── sql_queries.py              # create, drop, insert, select queries for defined tables

Follow the instruction below:

1. Create Redshift cluster using command below:

    **Note 1**: keep in mind that your Redshift cluster should be in the same region as S3 buckets.

    ```
    python create_redshift_cluster.py --aws-profile <put your AWS profile here>
    ```

    ```
    python add_security_group.py --aws-profile <put your AWS profile here>
    ```

3. Create required tables:

    **Note 2**: make sure that *dwh.cfg* is populated with credentials to your Redshift cluster and required policies.

    ```
    python create_tables.py
    ```

4. Execute ETL pipeline using:
    ```
    python etl.py
    ```
