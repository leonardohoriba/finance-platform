from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
from api.bloomberg import BbgCurrencyUpdatePrices
import logging


# Global variables
dag_id = 'bloomberg_currency'
dag_params = {
    'start_date': Param((datetime.now() - timedelta(days=1)).strftime('%Y%m%d'), type="string"),
    'end_date': Param(datetime.now().strftime('%Y%m%d'), type="string"),
}

def extract(**kwargs):
    start_date = kwargs['dag_run'].conf.get('start_date')
    end_date = kwargs['dag_run'].conf.get('end_date')
    logging.info(f"start_date: {start_date}")
    logging.info(f"end_date: {end_date}")    
    bloomberg_api = BbgCurrencyUpdatePrices(
        start_date=start_date,
        end_date=end_date
    )
    bloomberg_api.extract()

with DAG(
    dag_id=dag_id,
    schedule_interval='@once',
    catchup=False,
    start_date= datetime(2024,1,1),
    dagrun_timeout=timedelta(minutes=45),
    params=dag_params,
) as dag:
    
    extract_bloomberg = PythonOperator(
        provide_context=True,
        python_callable=extract,
        task_id='extract',
        dag=dag
    )
    
    extract_bloomberg