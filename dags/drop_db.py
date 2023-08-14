from airflow import DAG
from airflow.decorators import task
import datetime
from app.settings import DEFAULT_ARGS


with DAG(
    dag_id="drop_db",
    default_args=DEFAULT_ARGS,
    start_date = datetime.datetime(2023, 1, 1, 12),
    tags = ['hw_3', 'ETL'],
    schedule_interval = '@daily',
    ) as dag:
    
    from airflow.settings import engine
    from app.db.controller import Controller
    from app.settings import VERBOSE
    
    @task(task_id='drop_db')
    def drop_db():
        
        controller = Controller(engine, verbose=VERBOSE)
        return controller.destroy_db()
    
    drop_db()
    