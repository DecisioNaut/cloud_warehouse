# Udacity's Data Engineering With AWS Nano-Degree - Project: Data Warehouse

This is a **fictional project for lesson 2** of the Udacity's Data Engineering with AWS nano-degree course to be review by Udacity. Besides `README.md` the following scripts are relevant for the project:
- `sql_queries.py`
- `create_tables.py`
- `etl.py`  
There's of course also the `dwh.cfg` file which contains the configuration for the AWS resources, but in order to protect the credentials, this file is not included in the repository as it has been *"git-ignored"*.

In addition to the project itself, this repository also contains notebooks regarding the relevant exercises in the lesson which are not relevant for the project.

## The (Fictional) Task

A music streaming startup named Sparkify has grown their user base and song database and want to move their processes and data onto the cloud of AWS. Sparkify`s data already resides in S3-buckets, 
- in a directory of JSON logs on user activity on the app, as well as 
- in a directory with JSON metadata on the songs in the app.

In order to enable Sparkify to analyze the data, I've been asked to build a future-proof data warehouse in AWS. So, my task to create the infrastructure needed and to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for my colleagues from the analytics team to continue finding insights into what songs our users are listening to.

## Requirements


