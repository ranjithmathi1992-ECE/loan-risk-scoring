from faker import Faker
import pandas as pd
import random

fake = Faker()
random.seed(42)
Faker.seed(42)

EMPLOYMENT_TYPES = ["Salaried", "Self-Employed", "Business Owner", "Freelancer", "Government"]

LOAN_PURPOSES = ["Home Loan", "Car Loan", "Education Loan", "Personal Loan", "Business Loan", "Medical Loan", "Gold Loan"]

STATES = ["Tamil Nadu", "Karnataka", "Maharashtra", "Delhi", "Gujarat", "Rajasthan", "Uttar Pradesh", "West Bengal"]

BANKS = ["SBI", "HDFC", "ICICI", "Axis Bank", "Bank of Baroda", "Canara Bank", "Punjab National Bank", "Kotak Mahindra"]

def generate_loan_data(n=100000):
    records = []

    for i in range(n):
        age = random.randint(21, 65)
        annual_income = random.randint(120000, 5000000)
        loan_amount = random.randint(50000, 5000000)
        loan_tenure = random.choice([12, 24, 36, 48, 60, 84, 120])
        credit_score = random.randint(300, 900)
        existing_loans = random.randint(0, 5)
        employment_years = random.randint(0, 30)
        monthly_emi = round(loan_amount / loan_tenure, 2)
        dti_ratio = round((monthly_emi * 12) / annual_income, 4)
        ltv_ratio = round(loan_amount / (annual_income * 3), 4)
        missed_payments = random.randint(0, 12)
        collateral_value = round(loan_amount * random.uniform(0.5, 2.0), 2)

        risk_score = 0
        if credit_score < 550:
            risk_score += 3
        elif credit_score < 650:
            risk_score += 2
        elif credit_score < 750:
            risk_score += 1

        if dti_ratio > 0.5:
            risk_score += 3
        elif dti_ratio > 0.35:
            risk_score += 2
        elif dti_ratio > 0.20:
            risk_score += 1

        if missed_payments > 6:
            risk_score += 3
        elif missed_payments > 2:
            risk_score += 2
        elif missed_payments > 0:
            risk_score += 1

        if existing_loans > 3:
            risk_score += 2
        if employment_years < 1:
            risk_score += 2
        if ltv_ratio > 1.5:
            risk_score += 2

        if risk_score >= 8:
            risk_label = "HIGH"
        elif risk_score >= 4:
            risk_label = "MEDIUM"
        else:
            risk_label = "LOW"

        records.append({
            "applicant_id": f"APP{str(i+1).zfill(6)}",
            "applicant_name": fake.name(),
            "age": age,
            "gender": random.choice(["Male", "Female"]),
            "state": random.choice(STATES),
            "employment_type": random.choice(EMPLOYMENT_TYPES),
            "employment_years": employment_years,
            "annual_income": annual_income,
            "loan_amount": loan_amount,
            "loan_purpose": random.choice(LOAN_PURPOSES),
            "loan_tenure_months": loan_tenure,
            "loan_grade": random.choice(["A", "B", "C", "D", "E"]),
            "credit_score": credit_score,
            "existing_loans": existing_loans,
            "missed_payments": missed_payments,
            "monthly_emi": monthly_emi,
            "dti_ratio": dti_ratio,
            "ltv_ratio": ltv_ratio,
            "collateral_value": collateral_value,
            "bank": random.choice(BANKS),
            "risk_score": risk_score,
            "risk_label": risk_label,
            "default_flag": 1 if risk_label == "HIGH" else 0,
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    print("Generating 100,000 loan applicant records...")
    df = generate_loan_data(100000)
    df.to_csv("loan_applicants_100k.csv", index=False)
    print(f"Done! Generated {len(df)} records")
    print(f"Risk distribution:")
    print(df["risk_label"].value_counts().to_string())
    print(f"Default rate: {df['default_flag'].mean()*100:.1f}%")
