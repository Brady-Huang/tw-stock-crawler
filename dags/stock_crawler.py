from datetime import datetime, timedelta
from util.tw_stock import stock_crawler

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator

start_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day, 0, 0)

sc = stock_crawler()

default_args = {
    'owner': 'Yiming Huang',
    'depends_on_past': False,
    'start_date': start_date,
    # 'email': ['brady120915@gmail.com'],
    'description':'stock crawler',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_stock_info(**context):

    symbol_lst, category_sym_dic = sc.stock_info()
    
    return symbol_lst, category_sym_dic

def get_stock_data(**context):

    symbol_lst, _ = context['task_instance'].xcom_pull(task_ids='get_stock_info')
    stock_data_json = sc.get_each_stock_data(symbol_lst)

    return stock_data_json

def get_top_3_data_by_each_category(**context):

    _, category_sym_dic = context['task_instance'].xcom_pull(task_ids='get_stock_info')
    sc.get_top_3_data_by_category(category_sym_dic)

with DAG('tw_stock_crawler', default_args=default_args, schedule_interval="0 0 * * *") as dag:

    network_check = HttpSensor(
        task_id='network_check',
        http_conn_id='http_default',
        endpoint='',
        request_params={},
        response_check=lambda response: "httpbin" in response.text,
        poke_interval=5,
        dag=dag,
    )
    get_stock_info = PythonOperator(
        task_id='get_stock_info',
        python_callable=get_stock_info,
        provide_context=True
    )
    get_stock_data = PythonOperator(
        task_id='get_stock_data',
        python_callable=get_stock_data,
        provide_context=True
    )
    get_top_3_data_by_each_category = PythonOperator(
        task_id='get_top_3_data_by_each_category',
        python_callable=get_top_3_data_by_each_category,
        provide_context=True
    )
    network_check >> get_stock_info >> get_stock_data >> get_top_3_data_by_each_category