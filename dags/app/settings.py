import datetime


# Share settings

VERBOSE = True
log_file = True
log_path = 'logs'

# DataBase settings

DB_URI = 'postgresql://postgres:postgres@localhost:15432/etl'
THRESHOLD = 20

# HH api-client settings

API_URL = 'https://api.hh.ru/vacancies/'
SESSION_HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) Chrome/114.0.0.0',
    }
RETRY = 5
TIMEOUT = 2
QUERY = 'middle python developer'

# Okved parser settings

WORK_DIR = '/opt/airflow/files/'
OKVED_SOURCE = 'https://ofdata.ru/open-data/download/okved_2.json.zip'
OKVED_ZIP_FILE = f'{WORK_DIR}okved.zip'
OKVED_FILE = f'{WORK_DIR}okved_2.json'

EGRUL_SOURCE = 'https://ofdata.ru/open-data/download/egrul.json.zip'
EGRUL_ZIP_FILE = f'{WORK_DIR}egrul.zip'

DEFAULT_ARGS = {
    'owner': 'homutovan',
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=2),
}




