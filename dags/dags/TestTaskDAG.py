import pendulum
from airflow import DAG
import json
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import traceback
from sqlalchemy import create_engine
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.email_operator import EmailOperator

# Указывает на то, что DAG сделает 2 попытки повторного запуска с интервалом в 5 минут в случае ошибки
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Получение конфига DAG
with open('/opt/airflow/dags/configs/TestTaskDAGConfig.json', 'r') as f:
            config_data = json.load(f)


url = Variable.get("test_task_datasource")
sсhema_name = config_data["bd_object"]["sсhema_name"]
table_name = config_data["bd_object"]["table_name"]
year = config_data['start_date']['year']
month = config_data['start_date']['month']
day = config_data['start_date']['day']
tz = config_data['start_date']['tz']

with DAG(
        'TestTaskDAG',
        description='DAG для загрузки случайных данных в БД (ТЗ)',
        schedule_interval=timedelta(hours=12), # Задание интервала отработки DAG в 12 часов
        start_date=pendulum.datetime(year, month, day, tz=tz), # Дата начало работы DAG
        catchup=False, #Выполнение DAG только для текущего интервала времени
        tags=['ETL', 'TestTaskDAG'],
) as dag:

    # Функция для PythonOperator, которая считывает данные и записывает их в БД
    def get_and_insert_data(): 
        print('Запрос для получения данных из источника')
        response = requests.request("GET", url)
        print(f"Запрос вернул статус {response.status_code}")
        if (response.status_code != 200):
            return
        print('Данные успешно получены из источника')


        json_data = json.loads(response.text)
        print('Формирование датафрейма из полученных данных')
        df = pd.DataFrame(json_data)
        pg_connect = BaseHook.get_connection(conn_id='my_pg_connect') # Получение параметров коннекта к БД, которые вшиты в конфиг Airflow
        engine = create_engine(f"postgresql://{pg_connect.login}:{pg_connect.password}@{pg_connect.host}:{pg_connect.port}/{pg_connect.schema}")
        print("Подключение к БД прошло успешно")
        df.to_sql(name=table_name, con=engine, schema=sсhema_name, if_exists='append', index=False) # Автоматическое создание таблицы в БД через Pandas и sqlalchemy
        print(f"Данные успешно добавлены в таблицу {sсhema_name}.{table_name}")

    # Я решил использовать один оператор для всего процесса, хоть он и выполняет две логически разные операции (получение и запись данных)
    # Альтернативный вариант был: использовать http-оператор для получения данных, запись этих данных в системную БД Airflow через механизм x-com
    # и в дальнейшем считывать эту информацию от туда для использования при записи данных через Python оператор (используя те же механизмы, что и в get_and_insert_data())
    # Первый вариант выбрал по причине того, что не видел смысла в использовании дополнительной "прокладки", которая сделала бы текущий и так несложный код красивее
    # но при увеличении объема данных могла бы повлиять на производительность (x-com, по моему мнению, стоит использовать при работе с небольшими промежуточными данными, вроде токенов-авторизации или флагов)
    GetAndInsertData = PythonOperator(
    task_id='get_and_insert_data',
    python_callable=get_and_insert_data,
    dag=dag
    )
    
    

GetAndInsertData