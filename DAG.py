from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 16),
    'depends_on_past': False,
    'email': ['adeel.hamayoun@live.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('cricket_stats',
         default_args=default_args,
         description='Daily extraction of ranking data from cricbuzz API ',
         schedule='@daily',
         catchup=False
         ) as dag:

    end = EmptyOperator(task_id='end',
                        queue="general_workers")
    start = EmptyOperator(task_id='start',
                          queue="general_workers")

    import_cricket_stats_ranking_data = BashOperator(
        task_id='import_cricket_stats_ranking_data',
        bash_command='python /home/airflow/gcs/dags/scripts/cricbuzz_api.py',
        dag=dag)

    start >> import_cricket_stats_ranking_data >> end