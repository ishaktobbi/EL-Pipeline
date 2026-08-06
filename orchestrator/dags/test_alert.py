# dags/test_slack_alerts_dag.py
from datetime import datetime, timedelta
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the callback function
from alerts import on_failure_slack_alert, on_sla_miss_slack_alert

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'retries': 0,  # Set to 0 so it fails immediately during testing
    'on_failure_callback': on_failure_slack_alert, # 🟢 DAG-wide failure callback
    'execution_timeout': timedelta(minutes=5),  # Fails task if it takes longer than 5 minutes
    # 'sla_miss_callback': on_sla_miss_slack_alert,  # 🟡 SLA miss callback
}

with DAG(
    dag_id='test_slack_failure_notification',
    default_args=default_args,
    start_date=datetime(2026, 8, 4),
    max_active_runs=1,
    schedule_interval='* * * * *',
    catchup=False,
    tags=['monitoring', 'test'],
    sla_miss_callback=on_sla_miss_slack_alert,  # 🟡 DAG-wide SLA miss callback
) as dag:

    def intentional_failure():
        
        print("Starting task... sleeping for 2 minutes (120 seconds)...")
        time.sleep(120)
        print("Task completed successfully!")
        
    fail_task = PythonOperator(
        task_id='simulate_task_failure',
        python_callable=intentional_failure,
        sla=timedelta(seconds=60),  # Set SLA to 60 seconds for testing
    )

    fail_task