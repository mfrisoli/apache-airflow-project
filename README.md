# APACHE AIRFLOW PROJECT
## SPARKIFY DATASET: APACHE AIRFLOW PIPELINE TO AWS REDSHIFT

Apache Airflow pipeline design in accordance of Sparkify Dataset requirements.

This pipeline extracts from S3 and loads into AWS Redshift

Index:
1. Files and Requirements
2. Schema
3. How to Run

## 1 Files and Requirements

### 1.1 Files:

- `udac_example_dag.py`: Main DAG file
- `Operators`: Folder with custom Apache Airflow Python Operators
- `create_tables.sql`: SQL statements to create the staging and Star Schema tables.
- `sql_qieries.py`: Class with modularized SQL queries

### 1.2 Redshift Cluster:

For instructions on how to create and start an AWS Redshift cluster, please follow this github repository: [Datawharehouse with Redshift](https://github.com/mfrisoli/data-wharehouse-redshift/blob/main/README.md)

### 1.3 Technologies
- python3
- AWS Redshift
- Enviroment running Apache Airflow
- airflow

## 2 Schema

The star schema is the simplest style of data mart schema and is the approach most widely used to develop data warehouses and dimensional data marts. The star schema consists of one or more fact tables referencing any number of dimension tables. The star schema is an important special case of the snowflake schema, and is more effective for handling simpler queries <br>
Source: [Wikipedia](https://en.wikipedia.org/wiki/Star_schema)

#### **Fact Table**
- songplays - records in log data associated with song plays i.e. records with page NextSong.
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

#### **Dimension Tables**
- users - users in the app.
  - user_id, first_name, last_name, gender, level
- songs - songs in music database.
  - song_id, title, artist_id, year, duration
- artists - artists in music database
  - artist_id, name, location, latitude, longitude.
- time - timestamps of records in songplays broken down into specific units.
  - start_time, hour, day, week, month, year, weekday.

![Star Schema](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/339318/1586016120/Song_ERD.png "Star Schema")
Source: ***Udacity***

## 3 How to Run:

a) Dowload the repository to a machine that has Apache Airflow installed.

b) Once you have created your Redshift Cluster, get the following parameters:<br>

USER:
- IAM ARN ROLE

CLUSTER:
- Host
- DB Name
- DB User
- DB Password
- DB Port

Your should also have at hand your AWS user details with programmatic access.<br>

c) to start the Apache Airflow server UI run the following command: `/opt/airflow/start.sh`<br>

d) Create the Variables that will be used by the Hooks<br>

e) Turn the Dag to ON and it should start executing according to the schedule<br> 


### ****IMPORTANT: Make sure to delete your Redshift cluster and IAM client at the end as this incurs in cost.****<br>

Helpful reference: https://github.com/shravan-kuchkula/