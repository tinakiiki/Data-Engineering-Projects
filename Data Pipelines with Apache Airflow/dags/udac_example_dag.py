from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018,11,1),
    'end_date' :  datetime(2018,12,1),
    'depends_on_past': False,
    'email_on_retry' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup': False
    
}


dag = DAG('sparkify_etl_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key = "log_data",
    provide_context=True,
    aws_conn_id='aws_credentials',
    region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A",
    provide_context = True,
    aws_conn_id='aws_credentials',
    region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songplays",
    sql_insert_query=SqlQueries.songplay_table_insert,
    append=True
    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="users",
    sql_insert_query=SqlQueries.user_table_insert,
    append = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="songs",
    sql_insert_query=SqlQueries.song_table_insert,
    append = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="artists",
    sql_insert_query=SqlQueries.artist_table_insert,
    append = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="time",
    sql_insert_query=SqlQueries.time_table_insert,
    append = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["artists","users","time","songs","songplays"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator