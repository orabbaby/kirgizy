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
    schedule='0 0 * * *',
    catchup=False,
    concurrency=2,
    start_date=pendulum.yesterday("Europe/Moscow"),
    max_active_runs=1,
    tags=['facts', 'rkeeper', 'dt1'],
)
def my_first_dag():
    import pandas as pd

    k = [i for i in range(10)]

    @task
    def my_first_task(z, x, **kwargs):

        print(z, x)

        ti = kwargs["ti"]

        ti.xcom_push("task", 5)

        
        d = pd.DataFrame({})
        
        print("pipik kiil samy hofyi")
    
    @task
    def my_second_task( **kwargs):
        var = kwargs["params"]["amount"]
        print(var)
        ti = kwargs["ti"]

        r = ti.xcom_pull(task_ids="my_first_task", key="task")

        print(r)

        return sum([i for i in range(10)])

    (
        my_first_task.partial(z=2).expand(x=k)
        >> my_second_task()
    )

my_first_dag = my_first_dag()