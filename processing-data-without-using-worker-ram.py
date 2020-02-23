import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from time import time
import sql_statements

import psycopg2

def load_trip_data_to_redshift(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    connection = redshift_hook.get_conn()
    cur = connection.cursor()
    logging.info(f"redshift_hook connection: { connection }")
    
    execution_date = kwargs["execution_date"]
    year=execution_date.year
    month=execution_date.month
    
    s3_location = f"s3://udacity-dend/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
    
    t0 = time()
    

    query = ("""
        copy trips from '{}'
        iam_role '{}' region 'us-west-2'
        IGNOREHEADER 1
        DELIMITER ',';
    """).format(s3_location, 'arn:aws:iam::850186040772:role/dwhRole')
    logging.info(f"executing~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~: {query}")
    cur.execute(query)
    connection.commit()

    loadTime = time() - t0

    logging.info("===~~~~~~~~~~~~~~~~ DONE IN: {0:.2f} sec\n".format(loadTime))
    
    
    connection.close()


def load_station_data_to_redshift(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    connection = redshift_hook.get_conn()
    cur = connection.cursor()
    logging.info(f"redshift_hook connection: { connection }")
    
    
    t0 = time()
    

    query = ("""
        copy stations from '{}'
        iam_role '{}' region 'us-west-2'
        IGNOREHEADER 1
        DELIMITER ',';
    """).format("s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv", 'arn:aws:iam::850186040772:role/dwhRole')
    logging.info(f"executing~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~: {query}")
    cur.execute(query)
    connection.commit()

    loadTime = time() - t0

    logging.info("===~~~~~~~~~~~~~~~~ DONE IN: {0:.2f} sec\n".format(loadTime))
    
    
    connection.close()


def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 rows")
    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


dag = DAG(
    'lesson2.exercise4',
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 3, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    max_active_runs=1
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

copy_trips_task = PythonOperator(
    task_id='load_trips_from_s3_to_redshift',
    dag=dag,
    python_callable=load_trip_data_to_redshift,
    provide_context=True,
)

check_trips = PythonOperator(
    task_id='check_trips_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'trips',
    }
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = PythonOperator(
    task_id='load_stations_from_s3_to_redshift',
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

check_stations = PythonOperator(
    task_id='check_stations_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'stations',
    }
)

create_trips_table >> copy_trips_task
create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
copy_trips_task >> check_trips

