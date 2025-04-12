from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime
import random

# Load dataset
df = pd.read_csv("bank_transactions_data_2.csv")

# Ensure required columns exist
required_columns = ["TransactionID", "AccountID", "TransactionAmount", "TransactionDate", "TransactionType"]
if not all(col in df.columns for col in required_columns):
    raise ValueError("Dataset is missing required columns!")

# Convert TransactionDate to correct format
df["TransactionDate"] = pd.to_datetime(df["TransactionDate"], errors="coerce")
df.dropna(subset=["TransactionDate"], inplace=True)  # Remove rows with invalid dates
df["TransactionDate"] = df["TransactionDate"].dt.strftime("%Y-%m-%d %H:%M:%S")  # Standardize format

# Ensure TransactionAmount is numeric
df["TransactionAmount"] = pd.to_numeric(df["TransactionAmount"], errors="coerce")
df.dropna(subset=["TransactionAmount"], inplace=True)  # Remove invalid amounts

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "bank-------transactions"

# Stream transactions in real time
while True:
    # Simulate random transaction streaming
    transaction = df.sample(n=1).to_dict(orient="records")[0]
    
    # Randomly modify the transaction amount (to make streaming look real)
    transaction["TransactionAmount"] = round(transaction["TransactionAmount"] * (0.9 + random.uniform(0, 0.2)), 2)
    
    # Send to Kafka
    producer.send(topic, value=transaction)
    print(f"Sent: {transaction}")
    
    time.sleep(random.uniform(0.5, 2))  # Randomized delay to simulate real-world traffic

producer.flush()
producer.close()
