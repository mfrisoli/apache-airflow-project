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
    'start_date': datetime(2019, 1, 12),
    #'end_date': datetime(2019,1,12)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
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
    arn_iam_role="arn_iam_role",
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
    arn_iam_role="arn_iam_role",
    json="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

#[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

#load_songplays_table >> [load_song_dimension_table,
#                         load_user_dimension_table,
#                         load_artist_dimension_table,
#                         load_time_dimension_table]

#[load_song_dimension_table,
# load_user_dimension_table,
# load_artist_dimension_table,
# load_time_dimension_table] >> run_quality_checks


#run_quality_checks >> end_operator

