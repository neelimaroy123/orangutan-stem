# from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

def load_data_to_redshift():
    # redshift_filename = "rmv1vendor.py" 
    # os.system(f"python {redshift_filename}")
    import rmv1vendor

start_date = pendulum.today('UTC').add(days=-1)

dag = DAG(
    dag_id='rmv1vendor_dag',
    start_date=start_date,
    schedule="@daily",
    # catchup=False
)

# load_data_task =
PythonOperator(
    task_id='load_data_to_redshift_task',
    python_callable=load_data_to_redshift,
    dag=dag
)

# load_data_task

# if __name__ == "__main__":
#     dag.cli()
