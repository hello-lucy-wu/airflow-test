import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def log_rider_under_18():
    redshift_hook = PostgresHook("redshift")

    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

    
def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


args = {
    'start_date': datetime.datetime.utcnow(),
}

with DAG(dag_id='lesson3.exercise2', default_args=args) as dag:

    create_oldest_task = PostgresOperator(
        task_id="create_oldest",
        sql="""
            BEGIN;
            DROP TABLE IF EXISTS older_riders;
            CREATE TABLE older_riders AS (
                SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
            );
            COMMIT;
        """,
        postgres_conn_id="redshift"
    )


    create_rider_under_18_task = PostgresOperator(
        task_id="create_rider_under_18",
        sql="""
            BEGIN;
            DROP TABLE IF EXISTS younger_riders;
            CREATE TABLE younger_riders AS (
                SELECT * FROM trips WHERE birthyear > 2000
            );
            COMMIT;
        """,
        postgres_conn_id="redshift"
    )

    bike_ridden_frequency_task = PostgresOperator(
        task_id="bike_ridden_frequency_task",
        sql="""
            BEGIN;
            DROP TABLE IF EXISTS city_station_counts;
            CREATE TABLE city_station_counts AS(
                SELECT city, COUNT(city)
                FROM stations
                GROUP BY city
            );
            COMMIT;
        """,
        postgres_conn_id="redshift"
    )

    log_rider_under_18_task = PythonOperator(
        task_id="log_rider_under_18",
        python_callable=log_rider_under_18
    )


    log_oldest_task = PythonOperator(
        task_id="log_oldest",
        python_callable=log_oldest
    )


    create_oldest_task >> log_oldest_task
    create_rider_under_18_task >> log_rider_under_18_task
    bike_ridden_frequency_task

