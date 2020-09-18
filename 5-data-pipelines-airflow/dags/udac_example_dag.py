from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,

    # to prevent execution of multiple DAGs
    'max_active_runs': 1,

    # adding some common arguments for the tasks within the DAG
    'redshift_conn_id': 'redshift',
    'aws_credentials_id': 'aws_credentials',
    'region': 'us-west-2'
}

data_quality_checks=[
    {'sql': "SELECT COUNT(*) FROM artists   WHERE artistid is null", 'expected_result': 0},
    {'sql': "SELECT COUNT(*) FROM users     WHERE userid   is null", 'expected_result': 0},
    {'sql': "SELECT COUNT(*) FROM songs     WHERE songid   is null", 'expected_result': 0}
    ]

with DAG("s3_to_redshift",
         default_args=default_args,
         description='Load and transform S3 data in Redshift with Airflow',
         schedule_interval='0 * * * *') as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table_name="staging_events",
        s3_path="s3://udacity-dend/log-data",
        json_format="s3://udacity-dend/log_json_path.json",
        provide_context=True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table_name="staging_songs",
        s3_path="s3://udacity-dend/song_data",
        provide_context=True
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        sql_query = SqlQueries.songplay_table_insert,
        table_name = 'songplays',
        truncate=False # better to append to fact tables
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_query = SqlQueries.user_table_insert,
        table_name = 'users',
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_query = SqlQueries.song_table_insert,
        table_name = 'songs',
        truncate=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_query = SqlQueries.artist_table_insert,
        table_name = 'artists',
        truncate=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_query = SqlQueries.time_table_insert,
        table_name = 'time',
        truncate=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        data_checks=data_quality_checks
    )

    end_operator = DummyOperator(task_id='Stop_execution')


# using list to set task dependecies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
