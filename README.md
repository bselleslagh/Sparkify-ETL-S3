# Sparkify Datalake ETL

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This script will read JSON song and log data from S3, transform this data into a star-schema using Apache Sparkn, and then output the data in .parquet files and store them on S3.

## Context

First the script will load all song data stored on S3, the files are partitioned by the first three letters of each song's track ID and contain metadata about the songs.

For example:

```json
{
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_location": "",
    "artist_longitude": null,
    "artist_name": "Line Renaud",
    "duration": 152.92036,
    "num_songs": 1,
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "year": 0
}
```

After processing the song dataset we will process the log files, the log files in the dataset are partitioned by year and month.

### Fact and dimention tables

After loading the song and log data we will transform this into a star-schema using the following fact and dimension tables:

#### Fact Table

1. songplays

   \- records in log data associated with song plays i.e. records with page NextSong

   - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables

1. users 

   \- users in the app

   - *user_id, first_name, last_name, gender, level*

2. Songs

   \- songs in music database

   - *song_id, title, artist_id, year, duration*

3. artists 

   \- artists in music database

   - *artist_id, name, location, lattitude, longitude*

4. time

   \- timestamps of records in songplays broken down into specific units

   - *start_time, hour, day, week, month, year, weekday*

## Using the ETL-script

Requirements:

- Python 3.7 or greater
- An AWS account with access to S3
- Access to a Spark Cluster to run the script on

Steps:

1. Enter your AWS access keys for S3 in the dl.cfg file
2. Add the input and output directory to the input_data and output_data variables in the etl.py script.
3. Run etl.py on a spark cluster.

### Included files

#### dl.cfg

Contains the AWS access keys for connecting to S3

#### etl.py

The script for running the ETL pipeline on a Apache Spark cluster

