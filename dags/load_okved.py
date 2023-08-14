from airflow import DAG
from airflow.decorators import task
import datetime
from app.settings import DEFAULT_ARGS, OKVED_FILE


with DAG(
    dag_id = 'load_okved_db',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    @task(task_id='load_okved_db')
    def load_okved():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        from app.extractor.file_extractor import FileReader
        
        controller = Controller(engine, verbose=VERBOSE)
        controller.add_type_of_business(FileReader.read_json_by_path(f'{OKVED_FILE}'))
    
    load_okved()
