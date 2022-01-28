from datetime import datetime, timedelta

from airflow import DAG
from udac_example_dag import default_args
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG('create_tables_dag',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',     
          start_date = datetime.now(),
        )

create_tables_task = PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql='create_tables.sql'
)

