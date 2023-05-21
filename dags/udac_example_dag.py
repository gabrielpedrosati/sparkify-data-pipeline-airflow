# import built-in libraries
from datetime import datetime, timedelta
import os
import configparser

# airflow modules
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# modules
from sql_queries import SqlQueries
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)


# configparser variables
config = configparser.ConfigParser()
config_dir = os.path.dirname(os.getcwd()) + "\config\s3.cfg"
config.read(config_dir)

S3_BUCKET = config.get("S3", "BUCKET")
S3_EVENTS_KEY = config.get("S3", "EVENTS_KEY")
S3_SONGS_KEY = config.get("S3", "SONGS_KEY")


default_args = {
    "owner": "Pedrosa",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 15),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False
}


dag = DAG('udac_example_dag',
          default_args = default_args,
          description = "Load and transform data in Redshift with Airflow",
          schedule_interval = "0 * * * *",
          tags = ["udacity"]
        )

start_operator = DummyOperator(task_id = "Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = S3_BUCKET,
    s3_key = S3_EVENTS_KEY,
    dag = dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = S3_BUCKET,
    s3_key = S3_SONGS_KEY,
    dag = dag
)

load_songplays_table = LoadFactOperator(
    task_id = "Load_songplays_fact_table",
    redshift_conn_id = "redshift",
    table = "songplays",
    append = False,
    sql_create = SqlQueries.songplay_table_create,
    sql_insert = SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = "Load_user_dim_table",
    redshift_conn_id = "redshift",
    table = "dim_user",
    append = False,
    sql_create = SqlQueries.user_dimension_table_create,
    sql_insert = SqlQueries.user_table_insert,
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = "Load_song_dim_table",
    redshift_conn_id = "redshift",
    table = "dim_song",
    append = False,
    sql_create = SqlQueries.song_dimension_table_create,
    sql_insert = SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = "Load_artist_dim_table",
    redshift_conn_id = "redshift",
    table = "dim_artist",
    append = False,
    sql_create = SqlQueries.artist_dimension_table_create,
    sql_insert = SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = "Load_time_dim_table",
    redshift_conn_id = "redshift",
    table = "dim_time",
    append = False,
    sql_create = SqlQueries.time_dimension_table_create,
    sql_insert = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id = "Run_data_quality_checks",
    redshift_conn_id = "redshift",
    table="dim_user",
    dag=dag
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator