from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'ranjith',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'loan_risk_pipeline',
    default_args=default_args,
    description='Bank Loan Default Risk Scoring Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 21),
    catchup=False
)

def extract_and_load():
    os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
    engine = create_engine('mysql+pymysql://root:YOUR_PASSWORD@localhost/loan_db')
    applicants_df = pd.read_sql("SELECT * FROM loan_applicants", engine)
    loans_df = pd.read_sql("SELECT * FROM loan_details", engine)
    history_df = pd.read_sql("SELECT * FROM loan_history", engine)
    applicants_df.to_csv('/tmp/loan_applicants.csv', index=False)
    loans_df.to_csv('/tmp/loan_details.csv', index=False)
    history_df.to_csv('/tmp/loan_history.csv', index=False)
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/applicants"])
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/loans"])
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/history"])
    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_applicants.csv", "/loan_risk/applicants/"])
    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_details.csv", "/loan_risk/loans/"])
    subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_history.csv", "/loan_risk/history/"])
    print("ETL Complete!")

task1 = PythonOperator(
    task_id='extract_load_to_hdfs',
    python_callable=extract_and_load,
    dag=dag
)

task2 = BashOperator(
    task_id='run_spark_risk_scoring',
    bash_command='source ~/.bashrc && spark-submit ~/projects/loan_risk/pyspark/risk_scoring.py',
    dag=dag
)

task3 = BashOperator(
    task_id='query_hive_warehouse',
    bash_command='hive -e "USE loan_dw; SELECT predicted_risk, COUNT(*) FROM fact_loan_risk GROUP BY predicted_risk;"',
    dag=dag
)

task1 >> task2 >> task3
