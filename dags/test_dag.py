from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import platform
import psutil
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=2)
}

def check_python_environment():
    env_info = {
        'python_version': platform.python_version(),
        'platform': platform.platform(),
        'cpu_count': psutil.cpu_count(),
        'memory_available': psutil.virtual_memory().available / (1024 * 1024 * 1024),  # GB
        'environment_variables': dict(os.environ)
    }
    print(f"Environment Information: {env_info}")
    return env_info

with DAG(
    'test_environment_dag',
    default_args=default_args,
    description='DAG for testing Airflow environment',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 10),
    catchup=False,
    tags=['test']
) as dag:

    # Test Python environment
    test_python = PythonOperator(
        task_id='test_python_environment',
        python_callable=check_python_environment
    )

    # Test bash environment
    test_bash = BashOperator(
        task_id='test_bash_environment',
        bash_command='echo "Current directory: $PWD"; ls -la; df -h'
    )

    # Test disk I/O
    test_disk = BashOperator(
        task_id='test_disk_io',
        bash_command='dd if=/dev/zero of=/tmp/test_file bs=1M count=100 && rm /tmp/test_file'
    )

    # Define task dependencies
    test_python >> test_bash >> test_disk
