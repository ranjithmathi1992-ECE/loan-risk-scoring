import os
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
os.environ['SPARK_HOME'] = '/opt/spark'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create Spark Session
# SparkSession = entry point to use Spark
# appName = name shown in Spark UI
# master("local[*]") = use all CPU cores on local machine
spark = SparkSession.builder \
    .appName("LoanRiskScoring") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Created!")

# Read data from HDFS
# spark.read.csv = reads CSV file into Spark DataFrame
# header=true = first row is column names
# inferSchema=true = auto detect data types
applicants = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/loan_risk/applicants/loan_applicants.csv")

loans = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/loan_risk/loans/loan_details.csv")

history = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/loan_risk/history/loan_history.csv")

print("Data loaded from HDFS!")
print(f"Applicants: {applicants.count()}")
print(f"Loans: {loans.count()}")
print(f"History: {history.count()}")

# JOIN all 3 tables together
# join = combines tables on matching column
# "applicant_id" = column to match on
# "inner" = only matching records from both tables
data = applicants \
    .join(loans, "applicant_id", "inner") \
    .join(history, "applicant_id", "inner")

print(f"Joined data: {data.count()} records")

# Create Risk Label
# This is our TARGET variable — what we want to predict
# Based on business rules:
# HIGH RISK = credit score < 600 OR previous defaults > 1 OR debt ratio > 0.8
# MEDIUM RISK = credit score < 700 OR previous defaults = 1 OR debt ratio > 0.5
# LOW RISK = everything else
data = data.withColumn(
    "risk_label",
    when(
        (col("credit_score") < 600) |
        (col("previous_defaults") > 1) |
        (col("debt_to_income_ratio") > 0.8),
        "HIGH"
    ).when(
        (col("credit_score") < 700) |
        (col("previous_defaults") == 1) |
        (col("debt_to_income_ratio") > 0.5),
        "MEDIUM"
    ).otherwise("LOW")
)

# Show risk distribution
print("\n=== RISK DISTRIBUTION ===")
data.groupBy("risk_label").count().show()

# Feature Engineering
# Convert categorical columns to numbers
# ML algorithms only understand numbers!
# StringIndexer converts text to numbers
# Example: "Male" -> 0, "Female" -> 1
gender_indexer = StringIndexer(
    inputCol="gender",
    outputCol="gender_index"
)

employment_indexer = StringIndexer(
    inputCol="employment_type",
    outputCol="employment_index"
)

education_indexer = StringIndexer(
    inputCol="education",
    outputCol="education_index"
)

purpose_indexer = StringIndexer(
    inputCol="loan_purpose",
    outputCol="purpose_index"
)

label_indexer = StringIndexer(
    inputCol="risk_label",
    outputCol="label"
)

# VectorAssembler combines all feature columns into one vector
# ML algorithms need all features in ONE column called "features"
# Think of it like putting all ingredients into one bowl! 🥣
assembler = VectorAssembler(
    inputCols=[
        "age",
        "income",
        "employment_years",
        "loan_amount",
        "loan_term",
        "interest_rate",
        "existing_loans",
        "credit_score",
        "previous_loans",
        "previous_defaults",
        "on_time_payments",
        "late_payments",
        "debt_to_income_ratio",
        "gender_index",
        "employment_index",
        "education_index",
        "purpose_index"
    ],
    outputCol="features"
)

# Random Forest Classifier
# One of the most powerful ML algorithms!
# numTrees = how many decision trees to build
# More trees = more accurate but slower
# Think of it like asking 20 experts instead of 1! 🌳
rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=20
)

# ML Pipeline
# Chains all steps together in order
# stages = list of steps to execute
# Think of it like assembly line in factory! 🏭
pipeline = Pipeline(stages=[
    gender_indexer,
    employment_indexer,
    education_indexer,
    purpose_indexer,
    label_indexer,
    assembler,
    rf
])

# Split data into training and testing
# 80% for training ML model
# 20% for testing how good the model is
# seed=42 = random seed for reproducibility
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
print(f"\nTraining data: {train_data.count()} records")
print(f"Testing data: {test_data.count()} records")

# Train the ML model
# pipeline.fit = learns patterns from training data
# Like a student studying before exam! 📚
print("\nTraining ML model...")
model = pipeline.fit(train_data)
print("Model trained successfully!")

# Make predictions on test data
# model.transform = applies learned patterns to new data
# Like student taking exam after studying! 📝
predictions = model.transform(test_data)

# Evaluate model accuracy
# MulticlassClassificationEvaluator = measures how accurate model is
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"\nModel Accuracy: {float(accuracy * 100):.2f}%")

# Make predictions on ALL data
all_predictions = model.transform(data)

# Add human readable risk label back
# prediction column has numbers (0,1,2)
# We convert back to LOW/MEDIUM/HIGH
final_results = all_predictions.withColumn(
    "predicted_risk",
    when(col("prediction") == 0.0, "LOW")
    .when(col("prediction") == 1.0, "MEDIUM")
    .otherwise("HIGH")
)

# Show results
print("\n=== LOAN RISK PREDICTIONS ===")
final_results.select(
    "applicant_name",
    "age",
    "income",
    "credit_score",
    "loan_amount",
    "loan_purpose",
    "previous_defaults",
    "debt_to_income_ratio",
    "risk_label",
    "predicted_risk"
).show(20, truncate=False)

# Show HIGH RISK applicants
print("\n=== HIGH RISK APPLICANTS ===")
final_results.filter(
    col("predicted_risk") == "HIGH"
).select(
    "applicant_name",
    "income",
    "credit_score",
    "previous_defaults",
    "loan_amount",
    "state"
).show(truncate=False)

# Summary by state
print("\n=== RISK SUMMARY BY STATE ===")
final_results.groupBy("state", "predicted_risk") \
    .count() \
    .orderBy("state") \
    .show()

# Summary by loan purpose
print("\n=== RISK SUMMARY BY LOAN PURPOSE ===")
final_results.groupBy("loan_purpose", "predicted_risk") \
    .count() \
    .orderBy("loan_purpose") \
    .show()

# Save results to HDFS
print("\nSaving results to HDFS...")
final_results.select(
    "applicant_id",
    "applicant_name",
    "age",
    "gender",
    "income",
    "employment_type",
    "credit_score",
    "loan_amount",
    "loan_purpose",
    "previous_defaults",
    "debt_to_income_ratio",
    "state",
    "risk_label",
    "predicted_risk"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/loan_risk/results/")

print("Results saved to HDFS!")
print("Risk Scoring Complete!")

spark.stop()
