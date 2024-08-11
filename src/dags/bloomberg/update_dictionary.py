from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from helpers.dictionary import UpdateDictionary


# Global variables
dag_id = 'update_dictionary'

def update(**kwargs):
    update_dict = UpdateDictionary()
    update_dict.update_all()

with DAG(
    dag_id=dag_id,
    schedule_interval='@once',
    catchup=False,
    start_date= datetime(2024,1,1),
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    
    update_dictionary = PythonOperator(
        provide_context=True,
        python_callable=update,
        task_id='update',
        dag=dag
    )
    
    update_dictionary