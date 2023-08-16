from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import datetime
from app.settings import DEFAULT_ARGS, EGRUL_SOURCE, EGRUL_ZIP_FILE, OKVED_FILE, OKVED_ZIP_FILE, OKVED_SOURCE, WORK_DIR


with DAG(
    dag_id = 'calculate_skills',
    default_args = DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    empty_task1 = EmptyOperator(task_id='empty_task1')
    
    task1_1 = BashOperator(
        task_id='download_okved_file',
        bash_command=f'wget {OKVED_SOURCE} -O {OKVED_ZIP_FILE}',
    )
    
    task1_2 = BashOperator(
        task_id='unzip_okved_file',
        bash_command=f'unzip -o {OKVED_ZIP_FILE} -d {WORK_DIR}',
    )
    
    task1_3 = BashOperator(
        task_id='delete_okved_zip_file',
        bash_command=f'rm {OKVED_ZIP_FILE}',
    )
    
    task1_4 = BashOperator(
        task_id='delete_okved_json_file',
        bash_command=f'rm {OKVED_FILE}',
    )
    
    task2_1 = BashOperator(
        task_id='download_egrul_file',
        bash_command=f'wget {EGRUL_SOURCE} -O {EGRUL_ZIP_FILE}',
    )
    
    task2_2 = BashOperator(
        task_id='delete_egrul_zip_file',
        bash_command=f'rm {EGRUL_ZIP_FILE}',
    )
    
    @task(task_id='init_db')
    def init_db():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        
        controller = Controller(engine, verbose=VERBOSE)
        return controller.create_db()
    
    @task(task_id='load_okved_db')
    def load_okved():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        from app.extractor.file_extractor import FileReader
        
        controller = Controller(engine, verbose=VERBOSE)
        controller.add_type_of_business(FileReader.read_json_by_path(f'{OKVED_FILE}'))
    
    @task(task_id='load_egrul_db') 
    def load_egrul():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        from app.extractor.file_extractor import CustomReader
        
        controller = Controller(engine, verbose=VERBOSE)
        
        for item in CustomReader.read_zip(EGRUL_ZIP_FILE):
            controller.add_company_dir([item], commit=False)
        
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
        
    @task(task_id='calculate_skills')
    def calculate_skills():
        from airflow.settings import engine
        from app.db.controller import Controller
        from app.settings import VERBOSE
        
        controller = Controller(engine, verbose=VERBOSE)
        controller.calculate_top_skills()
        
    
    chain(
        init_db(), 
        [
            task1_1,
            task2_1
            ],
        [
            task1_2,
            empty_task1
            ],
        load_okved(),
        [
            load_egrul(),
            load_hh_vacancy()
            ],
        calculate_skills(),
        [task1_3, task1_4, task2_2]
        )
