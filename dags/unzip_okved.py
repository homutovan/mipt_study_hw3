from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import datetime
from app.settings import DEFAULT_ARGS, OKVED_ZIP_FILE, WORK_DIR


with DAG(
    dag_id = 'unzip_okved_file',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    task1_2 = BashOperator(
        task_id='unzip_okved_file',
        bash_command=f'unzip -o {OKVED_ZIP_FILE} -d {WORK_DIR}',
    )
    
    task1_2
    