from airflow.decorators import task, dag
import pendulum
from datetime import timedelta

default_args = {
    "owner": "nifiservice",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "params": {
        "amount": 20
    }
}

@dag(
    dag_id='my_first_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    concurrency=2,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['facts', 'rkeeper', 'dt1'],
)
def my_first_dag():

    @task
    def my_first_task():
        print("pipik kiil samy hofyi")
    
    @task
    def my_second_task():
        for i in range(10):
            return(sum(i))

    (
        my_first_task()
        >> my_second_task()
    )

my_first_dag = my_first_dag()