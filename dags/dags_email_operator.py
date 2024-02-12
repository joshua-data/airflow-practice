from airflow import DAG
from airflow.operators.email import EmailOperator

import datetime
import pendulum

with DAG(
    dag_id = 'dags_email_operator',
    schedule = '0 8 1 * *',
    start_date = pendulum.datetime(2023, 3, 1, tz='Asia/Seoul'),
    catchup = False
) as dag:

    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'joshuajkim413@gmail.com',
        subject = 'Airflow Success Email',
        html_content = 'Work has been successfully done in Airflow!'
    )