from airflow import DAG
from airflow.operators.python import PythonOperator

from common.common_func import regist2

import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_with_op_kwargs',
    schedule = '30 6 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz='Asia/Seoul'),
    catchup = False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable = regist2,
        op_args = ['jskim', 'man', 'kr', 'seoul'],
        op_kwargs = {
            'email': 'jskim@google.com',
            'phone': '010'
        }
    )

    regist2_t1