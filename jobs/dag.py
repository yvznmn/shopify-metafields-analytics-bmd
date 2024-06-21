from airflow import DAG
from airflow.operators.python import PythonOperator

def task1_function():
  # Your Python code for task 1
  print(1)

def task2_function():
  # Your Python code for task 2
  print(2)

with DAG(dag_id="my_dag") as dag:

  task1 = PythonOperator(
      task_id="task1",
      python_callable=task1_function,
  )

  task2 = PythonOperator(
      task_id="task2",
      python_callable=task2_function,
      upstream_from_task=task1
  )
