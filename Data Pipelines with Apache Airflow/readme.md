# Sparkify Project

## Project Description
A startup called Sparkify wants to set up their ETL to run regularly using Airflow.
This project involves using Airflow to create tables, load data and run data quality checks.

## Prerequisites
Working knowledge of SQL, Python and Airflow.

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

Songplays table is updated by appending rows since it is a larger fact table while the other dimension tables are updated by deleting the table and writing new records.

## ETL
The DAG defines a start and end date which informs airflow on when the load duration should be. Airflow automatically sets up backfill tasks for each schedule interval between start date and now.
The udac_example_dag.py script is used to process the define the tasks and call an operator in each task to execute the task. Each operator is defined in plugins>>operators and draws its inputs from the task in the DAG.
The staging events table is loaded with context which informs Airflow of the file structure which includes execution date, month and year defined in the operator in stage_redshift.py. This ensures that each file for a given execution date is loaded.


## Instructions: How to run the code
1.Connect to Airlow and turn the DAG on, then click the play icon to trigger the DAG