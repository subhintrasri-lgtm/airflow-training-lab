from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
import logging

default_args = {
    'owner': 'airflow',
}

with DAG (
    'my_1st_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2021, 7, 9),
    tags = ['ETL','example', 'training'],
    catchup=False,
) as dag :

    t1 = DummyOperator(
        task_id='my_1st_task',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='echo_task',
        bash_command='echo hello',
        dag=dag
    )

    def goodjob():
        print("Good job")
        return 'End.'

    t3 = PythonOperator(
        task_id='say_goodjob',
        python_callable=goodjob,
        dag=dag
    )

    def print_log_messages():
        logging.debug('This is a debug message')
        logging.info('This is an info message')
        logging.warning('This is a warning message')
        logging.error('This is an error message')
        logging.critical('This is a critical message')
        return 'Whatever is returned also gets printed in the logs'

    t4 = PythonOperator(
        task_id='print_log_messages',
        python_callable=print_log_messages,
        dag=dag,
    )

t1 >> [t2 ,t3] >> t4