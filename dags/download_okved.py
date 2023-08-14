from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import datetime
from app.settings import DEFAULT_ARGS, OKVED_ZIP_FILE, OKVED_SOURCE


with DAG(
    dag_id = 'download_okved_file',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    task1_1 = BashOperator(
        task_id='download_okved_file',
        bash_command=f'wget {OKVED_SOURCE} -O {OKVED_ZIP_FILE}',
    )

    task1_1
    