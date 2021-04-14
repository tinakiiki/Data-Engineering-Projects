from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date':datetime(2020, 3, 15),
    'depends_on_past': False,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5) 
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='None'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_path="log_data.json",
    s3_access_key_id=AWS_KEY,
    s3_secret_access_key=AWS_SECRET,
    region="us-east",
    sql_create_statement=SqlQueries.staging_events_table_create
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_path="song_data/A/A'",
    s3_access_key_id=AWS_KEY,
    s3_secret_access_key=AWS_SECRET,
    region="us-east",
    sql_create_statement=SqlQueries.staging_songs_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songplays",
    sql_insert_query=SqlQueries.songplay_table_insert,
    sql_create_query=SqlQueries.staging_events_table_create,                                                 column_names=['songplay_id', 'start_time', 'userid', 'level', 'song_id', 'artist_id', 'sessionid',  'location', 'useragent'],
    primary_key = 'songplay_id'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="users",
    sql_insert_query=SqlQueries.user_table_insert,
    sql_create_query=SqlQueries.user_table_create,
    column_names =['userid', 'firstname', 'lastname', 'gender', 'level']
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_insert_query=SqlQueries.song_table_insert,
    sql_create_query=SqlQueries.song_table_create,
    column_names =['song_id', 'title', 'artist_id', 'year', 'duration']
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_insert_query=SqlQueries.artist_table_insert,
    sql_create_query=SqlQueries.artist_table_create,
    column_names =['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    redshift_conn_id="redshift",
    destination_table="time",
    sql_insert_query=SqlQueries.time_table_insert,
    sql_create_query=SqlQueries.time_table_create,
    column_names =['timestamp', 'hour', 'day', 'week','month' ,'year' , 'weekday']
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_list=['staging_events','staging_songs','songplays','users','songs,artists','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
stage_events_to_redshift >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_user_dimension_table >> load_song_dimension_table
load_song_dimension_table >> load_artist_dimension_table
load_artist_dimension_table >> load_time_dimension_table
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator