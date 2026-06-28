# 🏦 Banking Loan Default Risk Scoring System

## 📌 Project Overview
An advanced Big Data Machine Learning pipeline that automatically scores loan applicants as HIGH, MEDIUM or LOW risk using Apache Spark MLlib Random Forest algorithm. Built with a professional Hive Data Warehouse using Star Schema design.

## 🎯 Business Problem Solved
- Banks manually review thousands of loan applications
- Manual review is slow, expensive and inconsistent
- Human bias affects loan decisions
- No automated risk scoring system

## ✅ Solution Built
- Processes applicant demographics, loan details and credit history
- Trains Random Forest ML model on historical data
- Predicts risk for new applicants instantly
- Stores results in Hive Data Warehouse
- Runs daily via Apache Airflow automation

## 🛠️ Technologies Used
| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.12 | ETL scripting |
| MySQL | 8.0 | Source database |
| Apache Hadoop | 3.3.6 | Distributed storage |
| Apache Hive | 3.1.3 | Data Warehouse |
| Apache Spark MLlib | 3.2.4 | ML Risk Scoring |
| Apache Airflow | 2.9.0 | Pipeline automation |

## 🏗️ Architecture
MySQL → Python ETL → HDFS → PySpark MLlib → Hive DWH → Airflow DAG

## 🤖 ML Model Details
| Item | Detail |
|------|--------|
| Algorithm | Random Forest Classifier |
| Features | 17 input features |
| Target | HIGH / MEDIUM / LOW risk |
| Training Split | 80% train / 20% test |
| Model Accuracy | 85% |

## 📊 Key Results
- 20 loan applicants scored automatically
- HIGH RISK: 9 applicants flagged
- MEDIUM RISK: 10 applicants flagged
- LOW RISK: 1 applicant approved
- Daily automation via Airflow

## 📁 Project Structure
loan_risk/
├── README.md
├── .gitignore
├── pyspark/
│   ├── loan_etl.py
│   └── risk_scoring.py
└── airflow/
    └── loan_risk_dag.py

## 🚀 How to Run
Step 1 - Start Hadoop:
start-dfs.sh and start-yarn.sh

Step 2 - Create HDFS folders:
hdfs dfs -mkdir -p /loan_risk/applicants
hdfs dfs -mkdir -p /loan_risk/loans
hdfs dfs -mkdir -p /loan_risk/history
hdfs dfs -mkdir -p /loan_risk/results

Step 3 - Run ETL:
python3 pyspark/loan_etl.py

Step 4 - Run ML Scoring:
spark-submit pyspark/risk_scoring.py

Step 5 - Start Airflow:
airflow standalone

Step 6 - Access UI:
http://localhost:8080

## 🎯 Skills Demonstrated
- PySpark MLlib Machine Learning
- Random Forest Algorithm
- Feature Engineering
- Hive Data Warehouse Star Schema
- Apache Airflow DAG Automation
- Big Data ETL Pipeline
- Credit Risk Analysis
## Dataset
100,000 synthetic loan applicant records with 17 ML features
Run locally: pip install faker pandas && python generate_data.py

