from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. Define the structural blueprint of our pipeline
with DAG(
    dag_id='my_first_clean_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # This means "Only run when I manually click play"
    catchup=False
) as dag:

    # 2. Define Task 1: Print a message using bash terminal commands
    say_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World! Airflow is running under my Mac profile!"'
    )

    # 3. Define Task 2: Print a second message
    say_goodbye = BashOperator(
        task_id='print_goodbye',
        bash_command='echo "Goodbye! Task sequence complete!"'
    )

    # 4. Set the execution flowchart direction (Task 1 MUST run before Task 2)
    say_hello >> say_goodbye