from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'mfrisoli',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'provide_context': True    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Airflow Global Variables
# arn_iam_role
# s3_bucket: 
# Hooks
# redshift
# aws_credentials

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/{ds}-events.json",
    region="us-west-2",
    arn_iam_role="arn:aws:iam::879294216748:role/dwhRole",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    arn_iam_role="arn:aws:iam::879294216748:role/dwhRole",
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    table="songplays",
    columns="""
        playid,
        start_time,
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent"""
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    table="users",
    columns="""
        userid,
        first_name,
        last_name,
        gender,
        level
    """
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    table="songs",
    columns="""
        songid,
        title,
        artistid,
        year,
        duration
     """
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
    columns="""
        artistid,
        name,
        location,
        lattitude,
        longitude
    """
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table="time",
    columns="""
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    """
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['artists',
           'songplays',
           'songs',
           'staging_events',
           'staging_songs',
           'time',
           'users'
           ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

[load_song_dimension_table,
 load_user_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks


run_quality_checks >> end_operator

