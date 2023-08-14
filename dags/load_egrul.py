from airflow import DAG
from airflow.decorators import task
import datetime
from app.settings import DEFAULT_ARGS, EGRUL_ZIP_FILE


with DAG(
    dag_id = 'load_egrul_db',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    @task(task_id='load_egrul_db') 
    def load_egrul():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        from app.extractor.file_extractor import CustomReader
        
        controller = Controller(engine, verbose=VERBOSE)
        
        for item in CustomReader.read_zip(EGRUL_ZIP_FILE):
            controller.add_company_dir([item], commit=False)
        
        
    load_egrul()
    