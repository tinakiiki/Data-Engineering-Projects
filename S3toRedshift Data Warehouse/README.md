# Sparkify Project

## Project Description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project involves building an ETL pipeline for a database hosted on Redshift. The data is loaded from S3 into staging tables on Redshift and then SQL statements are used to create the analytics tables from these staging tables.

## Prerequisites
SQL, Jupyter notebooks, Python, AWS Redshift/S3 basics

## Schema design
A Redshift database named dev is created with fact and dimension tables to hold the required records. The create_tables.py and sql_queries.py scripts were used to implement this task.

### Fact Table
 The songplays table is designed to hold records in log data associated with song plays i.e. records with page NextSong and the following fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. The songplay_id is an auto generated primary key while start_time, user_id, song_id and artist_id are foreign keys to the dimension tables. 
 
### Dimension Tables
- users - has information on users in the app with fields: user_id, first_name, last_name, gender, level
- songs - contains songs in music database with fields: song_id, title, artist_id, year, duration
- artists - artists in music database with fields: artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units i.e start_time, hour, day, week, month, year, weekday

The primary keys for these tables are start_time, user_id, song_id and artist_id and are not being updated on conflict because the information on the artist and song is not expected to change.

## ETL

The ETL involves loading the files from S3 folders into two staging tables using the COPY command, followed by a SQL-SQL ETL to create destination tables and insert data from the staging tables into them. 

## Instructions: How to run the code
1. Start by inputting credentials in dwh.cfg, these credentials should be associated with a Redshift cluster on AWS and an IAM role which has read access to the s3 files as well as full access to the Redshift cluster. 
2. In the terminal type: "python create_tables.py" to run the python file create_tables.py. This script is used to create the  to create or drop tables in sql_queries.py. The tables listed in sql_queries.py are where the cleaned data will be logged.
3. Run "python etl.py" to process the files from the data folder and write the records to the tables created in the first step
4. Run test.ipynb in Jupyter notebooks to check that all the data has been logged.This file has some example queries to give a preview of the data and count the number of records returned. Restart this notebook at the end to close the connection.
5. For a full snapshot of the song data, please edit the path to SONG_DATA to 's3://udacity-dend/song_data' in the dwh.cfg file, this code uses a path to one file to shorten the run time 's3://udacity-dend/song_data/A/A'
6. Log onto the Redshift cluster on AWS and use the query editor to verify that data was successfully loaded in the tables, sample queries include 'SELECT count(*) FROM table_name' to count records in a table, also verify that all columns are loaded and have data in them.