from airflow import DAG
from airflow.decorators import task

import datetime
import pendulum

with DAG(
    dag_id = 'dags_python_with_macro',
    schedule = '10 0 * * *',
    start_date = pendulum.datetime(2023, 3, 1, tz='Asia/Seoul'),
    catchup = False
) as dag:
    
    @task(
            task_id = 'task_using_macro',
            templates_dict = {
                'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
            }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'No start_date'
            end_date = templates_dict.get('end_date') or 'No end_date'
            print(start_date)
            print(end_date)
    
    @task(task_id = 'task_direct_calc')
    def get_datetime_cal(**kwargs):
        
        from dateutil.relativedelta import relativedelta
        data_interval_end = kwargs['data_interval_end']

        prev_month_first_day = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_last_day = data_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        prev_month_first_day = prev_month_first_day.strftime('%Y-%m-%d')
        prev_month_last_day = prev_month_last_day.strftime('%Y-%m-%d')
        
        print(prev_month_first_day)
        print(prev_month_last_day)