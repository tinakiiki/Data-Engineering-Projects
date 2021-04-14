# Sparkify Project

## Project Description
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project involves building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Prerequisites
Working knowledge and appropriate installation of the following: SQL, Jupyter notebooks, Python, AWS Redshift/S3 and Spark

### Fact Table
 The songplays table is designed to hold records in log data associated with song plays i.e. records with page NextSong and the following fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. The songplay_id is an auto generated primary key while start_time, user_id, song_id and artist_id are foreign keys to the dimension tables. 
 
### Dimension Tables
- users - has information on users in the app with fields: user_id, first_name, last_name, gender, level
- songs - contains songs in music database with fields: song_id, title, artist_id, year, duration
- artists - artists in music database with fields: artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units i.e start_time, hour, day, week, month, year, weekday

The primary keys for these tables are start_time, user_id, song_id and artist_id and are not being updated on conflict because the information on the artist and song is not expected to change.

## ETL

The ETL involves reading files from S3, processing them into appropriate columns using Spark, then writing back the dimensional tables as parquet files into S3, for easy querying.

## Instructions: How to run the code
1. Start by inputting credentials in dwh.cfg, these credentials should be associated with an IAM role which has full Admin access to your cluster. 
2. In the terminal type: "python etl.py" to run the python file etl.py. This script is used to read the s3 date, preprocess the data with Spark, then write it back to s3.
3. Run "python etl.py" to process the files from the data folder and write the records to the tables created in the first step
4. If code runs OK, change song_data path to use the longer path which is commented out for a full ETL on the complete dataset