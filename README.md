# Udacity's Data Engineering With AWS Nano-Degree - Project: Data Warehouse

This is a **fictional project for lesson 2** of the **Udacity's Data Engineering with AWS nano-degree course** to be review by Udacity. Please find detailled instructions for the project in the file [`sparkify_Instructions_from_Udacity.md`](./sparkify_Instructions_from_Udacity.md).

**Table of Contents**
1. [The (Fictional) Task In A Nutshell](#1-the-fictional-task-in-a-nutshell)
2. [Structure of this Repository](#2-structure-of-this-repository)
3. [Usage](#3-usage)
4. [Data, Model and ETL](#4-data-model-and-etl)
    1. [Data](#41-data)
    2. [Model](#42-model)
    3. [ETL](#43-etl)

## 1. The (Fictional) Task In A Nutshell

A music streaming startup named Sparkify has grown their user base and song database and want to move their processes and data onto the cloud of AWS. Sparkify`s data already resides in S3-buckets, 
- in a directory of JSON logs on user activity on the app, as well as 
- in a directory with JSON metadata on the songs.
- For the JSON logs, there's also a corresponding JSON `log-json-path.json` file helping to parse the logs.

In order to enable Sparkify to analyze the data, I've been asked to build a data warehouse in AWS Redshift. So, my task to create the infrastructure needed and to build an ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for my colleagues from the analytics team to continue finding insights into what songs our users are listening to.

## 2. Structure of this Repository

Besides this `README.md` the following scripts are relevant for the project:
- `sql_queries.py`
- `create_tables.py`
- `etl.py`

Additionally, I've build a script using the `boto3` library to create the AWS resources needed for the project:
- `infra.py`

Futhermore, [`sparkify_exploration.ipynb`](./sparkify_exploration.ipynb) contains an extensive exploration of the data done locally with data downloaded from the S3-buckets before building the ETL pipeline.

There's also a `dwh.cfg` file which contains the configuration for the AWS resources. Of course, this does not contain AWS credentials needed to build the infrastructure. These are stored in a `.env` file which is not part of this repository.

Please note that, in addition to the project itself, this repository also contains notebooks regarding the relevant exercises in the lesson which are not relevant for the project.

## 3. Usage

## 4. Data, Model and ETL

### 4.1 Data

A typical JSON log file looks like this:
```
{
    'artist': 'Sound 5',
    'auth': 'Logged In',
    'firstName': 'Jacob',
    'gender': 'M',
    'itemInSession': 2,
    'lastName': 'Klein',
    'length': 451.21261,
    'level': 'paid',
    'location': 'Tampa-St. Petersburg-Clearwater, FL',
    'method': 'PUT',
    'page': 'NextSong',
    'registration': 1540558108796.0,
    'sessionId': 518,
    'song': 'Latin Static',
    'status': 200,
    'ts': 1542462343796,
    'userAgent': '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"',
    'userId': '73'
}
```
A typical JSON song file looks like this:
```
{
    'artist_id': 'ARLYGIM1187FB4376E',
    'artist_latitude': None,
    'artist_location': '',
    'artist_longitude': None,
    'artist_name': 'Joe Higgs',
    'duration': 162.82077,
    'num_songs': 1,
    'song_id': 'SOQOOPI12A8C13B040',
    'title': 'Wake up And Live',
    'year': 1975
}
```

### 4.2 Model

The database schema is a star schema with one fact table and four dimension tables. The fact table is `songplays` and the dimension tables are `users`, `songs`, `artists` and `time`:

**Fact Table: songplays**
| Column | Origin | Type | Comment |
| ------ | ------ | ---- | ------- |
| **songplay_id** | log_data itemInSession | NOT NULL SMALLINT | PRIMARY KEY |
| *start_time* | log_data ts | NOT NULL TIMESTAMP | REFERENCES time(start_time) |
| *user_id* | log_data userId | NOT NULL INTEGER | REFERENCES users(user_id) |
| level | log_data level | NOT NULL CHAR(4) |  |
| *song_id* | song_data song_id | NOT NULL CHAR(18) | REFERENCES songs(song_id) |
| *artist_id* | song_data artist_id | NOT NULL CHAR(18) | REFERENCES artists(artist_id) |
| **session_id** | log_data sessionId | NOT NULL INTEGER | PRIMARY KEY |
| location | log_data location | TEXT |  |


**Fact Table: songplays**
| Column | Type | Comment |
| ------ | ---- | ------- |
| **songplay_id**\* | NOT NULL SMALLINT | **PRIMARY KEY** from log JSON (itemInSession) |
| *start_time* | NOT NULL TIMESTAMP | REFERENCES time(start_time) |
| *user_id* | NOT NULL INTEGER | REFERENCES users(user_id) |
| level | NOT NULL CHAR(4) | free or paid from log JSON |
| *song_id* | NOT NULL CHAR(18) | REFERENCES songs(song_id) |
| *artist_id* | NOT NULL CHAR(18) | REFERENCES artists(artist_id) |
| **session_id**\* | NOT NULL INTEGER | **PRIMARY KEY** from log JSON |
| location | TEXT | from song JSON |
| user_agent | TEXT | from log JSON |  

\*Please note that songplay_id and session_id in combination are the unique primary keys of the fact table.
Please also note that there are logs without any userId and song information. These logs are not relevant for the analytics team and therefore not included in the fact table.  It appears as if they were generated when users log off.

**Dimension Table: users**
| Column | Type | Comment |
| ------ | ---- | ------- |
| **user_id** | NOT NULL INTEGER | PRIMARY KEY from log JSONs |
| first_name | TEXT | from log JSONS |
| last_name | TEXT | from log JSONS |
| gender | CHAR(1) | from log JSON |
| level\* | CHAR(4) | free or paid from log JSON |  

\**Please note that due to changes in user status, level represents the current level of the user and may differ from the level in the fact table which represents the level the song was heard with.*


**Dimension Table: songs**
| Column | Type | Comment |
| ------ | ---- | ------- |
| **song_id** | CHAR(18) | PRIMARY KEY from song JSON |
| title | TEXT | from song JSON |
| *artist_id* | CHAR(18) | REFERENCES artists(artist_id) |
| year | SMALLINT | from song JSON |
| duration | DECIMAL(10, 4) | from song JSON |

**Dimension Table: artists**
| Column | Type | Comment |
| ------ | ---- | ------- |
| **artist_id** | CHAR(18) | PRIMARY KEY from song JSON |
| name | TEXT | from song JSON |
| location | TEXT | from song JSON |
| lattitude | DECIMAL(8, 5) | from song JSON |
| longitude | DECIMAL(8, 5) | from song JSON |

**Dimension Table: time**
| Column | Type | Comment |
| ------ | ---- | ------- |
| **start_time** | TIMESTAMP | PRIMARY KEY from log JSON |
| hour | SMALLINT | to be calculated from start_time |
| day | SMALLINT | to be calculated from start_time |
| week | SMALLINT | to be calculated from start_time |
| month | SMALLINT | to be calculated from start_time |
| year | SMALLINT | to be calculated from start_time |
| weekday | SMALLINT | to be calculated from start_time |


### 4.3 ETL


