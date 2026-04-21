import os
os.environ['PATH'] = '/opt/hd/bin:/opt/hd/sbin:' + os.environ['PATH']
from sqlalchemy import create_engine
import pandas as pd
import subprocess

# Connect to MySQL
# create_engine builds connection string for MySQL
# mysql+pymysql = driver to use
# root = username, YOUR_PASSWORD = password
# localhost = server, loan_db = database name
engine = create_engine('mysql+pymysql://root:root@localhost/loan_db')

# Extract all 3 tables from MySQL into pandas DataFrames
# pd.read_sql runs SQL query and returns DataFrame
# Think of DataFrame like an Excel sheet in Python!
applicants_df = pd.read_sql("SELECT * FROM loan_applicants", engine)
loans_df = pd.read_sql("SELECT * FROM loan_details", engine)
history_df = pd.read_sql("SELECT * FROM loan_history", engine)

print(f"Extracted {len(applicants_df)} applicants")
print(f"Extracted {len(loans_df)} loans")
print(f"Extracted {len(history_df)} history records")

# Save to temporary CSV files
# /tmp = Linux temporary folder
# index=False = don't save row numbers
applicants_df.to_csv('/tmp/loan_applicants.csv', index=False)
loans_df.to_csv('/tmp/loan_details.csv', index=False)
history_df.to_csv('/tmp/loan_history.csv', index=False)

# Create HDFS directories
# subprocess.run = runs Linux command from Python
# hdfs dfs -mkdir -p = creates folder in HDFS
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/applicants"])
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/loans"])
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/history"])
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/results"])
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/loan_risk/warehouse"])

# Upload CSV files to HDFS
# hdfs dfs -put -f = upload file to HDFS
# -f = overwrite if exists
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_applicants.csv", "/loan_risk/applicants/"])
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_details.csv", "/loan_risk/loans/"])
subprocess.run(["hdfs", "dfs", "-put", "-f", "/tmp/loan_history.csv", "/loan_risk/history/"])

print("ETL Complete! All data loaded to HDFS!")
