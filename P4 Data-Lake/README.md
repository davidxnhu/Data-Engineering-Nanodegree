## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, We are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll be able to test our database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results.

## Project Description

In this project, We'll apply what we've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, we will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We'll deploy this Spark process on a cluster using AWS.

## Project Datasets

We'll be working with two datasets that reside in S3. Here are the S3 links for each:

* Song data: 
    s3://udacity-dend/song_data
    
* Log data: 
    s3://udacity-dend/log_data
    
## Song dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.  Here is an example of song file:

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


## Log Dataset
The second dataset consists of log files in JSON format generated by event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset we'll be working with are partitioned by year and month. Here is an example of log file:

![image1](img/log-data.png)

## Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

This database is extremely useful in the context of the startup, Sparkify, to analyze the user behaviours and improve the user experiences when they use the app. This will allow the analytics team to gain more insights from a set of dimensional tables.

For example, it could provide rankings e.g. popular songs, popular artists, etc.

In addition, the user behaviour could be identified, i.e. how much they will listen on each day, if they like to hear more on weekdays or weekends, how many would like to become a paid user, etc.

Therefore, the database provides a foundation for further data analysis processes.

## State and justify database schema design and ETL pipeline.

The star scheme is used for this database. It consists of one fact table (songplays) and references to four dimention tables (users, songs, artists, time).

**songplays**  - records in log data associated with song plays i.e. records with page NextSong
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month

**users** - users in the app
* userId, firstName, lastName, gender, level

**songs** - songs in music database
* song_id, title, artist_id, year, duration

**artists** - artists in music database
* artist_id, artist_name, artist_location, artist_latitude, artist_longitude

**time** - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday

ETL pipeline starts from reading song and log dataset on S3 which include the details of the user behaviours and the songs that the user could listen to, processign with Spark, and loading them back to S3. The fact table (songplays) provides the metric of the business process (here the user activity in the app). Further analysis could be proceeded with queries in the database.


## Repository files

**img** contains images for README.md

**etl.py** contains code for executing the queries which load JSON data from S3 bucket, process with spark and load them back to S3.

**dl.cfg** is the configuration file contains information about AWS account. 

**etl_1.ipynb** and **etl_2.ipynb** are used to help run and check different part of **etl.py**, then codes are copied back to etl.py.


## How to run

Create spark session, process song and log files, and load back to S3. They can be done by running:

``` 
python etl.py 
```