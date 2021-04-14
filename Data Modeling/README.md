# Sparkify Project

## Project Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project involves creating a Postgres database with tables,and an ETL pipeline  designed to optimize queries on song play analysis. 

## Prerequisites
Working knowledge of SQL, Jupyter notebooks and Python.

## Schema design
A postgres database named sparkify is created with fact and dimension tables to hold the required records. The create_tables.py and sql_queries.py scripts were used to implement this task.
### Fact Table
 The songplays table is designed to hold records in log data associated with song plays i.e. records with page NextSong and the following fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. The songplay_id is an auto generated primary key while start_time, user_id, song_id and artist_id are foreign keys to the dimension tables. 
### Dimension Tables
- users - has information on users in the app with fields: user_id, first_name, last_name, gender, level
- songs - contains songs in music database with fields: song_id, title, artist_id, year, duration
- artists - artists in music database with fields: artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units i.e start_time, hour, day, week, month, year, weekday

The primary keys for these tables are start_time, user_id, song_id and artist_id and are not being updated on conflict because the information on the artist and song is not expected to change.

## ETL
The etl.py script is used to process the data and write it to the tables described above. The script checks a sub folder for all json files and processes these files to format data and extract the required columns. The script then iterates through each record and writes it to the predefined table.


## Instructions: How to run the code
1. In the terminal type: python create_tables.py to run the python file create_tables.py. This script is used to create the database sparkify.db and to create or drop tables in sql_queries.py. The tables listed in sql_queries.py are where the cleaned data will be logged.
2. Run python etl.py to process the files from the data folder and write the records to the tables created in the first step
3. Run test.ipynb in Jupyter notebooks to check that all the data has been logged.This file has some example queries to give a preview of the data and count the number of records returned. Restart this notebook at the end to close the connection.