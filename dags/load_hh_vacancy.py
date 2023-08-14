from airflow import DAG
from airflow.decorators import task
import datetime
from app.settings import DEFAULT_ARGS


with DAG(
    dag_id = 'load_hh_vacancy',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    @task(task_id='load_hh_vacancy')
    def load_hh_vacancy():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE, QUERY
        from app.extractor.api_extractor import api_extractor
        from app.extractor.validators import Company, Vacancy
        
        controller = Controller(engine, verbose=VERBOSE)
        
        for item in api_extractor(QUERY):
            controller.add_company([Company(**item).dict()])
            controller.add_vacancy([Vacancy(**item).dict()])
            
            
    load_hh_vacancy()