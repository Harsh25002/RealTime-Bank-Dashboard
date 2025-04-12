from kafka import KafkaConsumer
import sqlite3
import json
import pandas as pd
from datetime import datetime

# Connect to SQLite database
conn = sqlite3.connect("transactions.db")
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TransactionID TEXT UNIQUE,
        AccountID TEXT,
        TransactionAmount REAL,
        TransactionDate TEXT,
        TransactionType TEXT,
        TransactionHour INTEGER
    )
""")
conn.commit()

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    "bank-------transactions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Consume messages and insert into database
for msg in consumer:
    data = msg.value
    
    try:
        # Ensure all required keys exist
        required_keys = ["TransactionID", "AccountID", "TransactionAmount", "TransactionDate", "TransactionType"]
        if not all(key in data for key in required_keys):
            print(f"Skipping invalid transaction: {data}")
            continue
        
        # Extract transaction hour
        try:
            transaction_hour = datetime.strptime(data["TransactionDate"], "%Y-%m-%d %H:%M:%S").hour
        except ValueError:
            print(f"Skipping invalid date format: {data['TransactionDate']}")
            continue
        
        # Insert into SQLite database
        cursor.execute("""
            INSERT OR IGNORE INTO transactions 
            (TransactionID, AccountID, TransactionAmount, TransactionDate, TransactionType, TransactionHour) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (data["TransactionID"], data["AccountID"], data["TransactionAmount"], data["TransactionDate"], data["TransactionType"], transaction_hour))
        
        conn.commit()
        print(f"Inserted: {data}")

    except Exception as e:
        print(f"Error processing transaction: {data} - {str(e)}")

conn.close()
from kafka import KafkaConsumer
import sqlite3
import json
import pandas as pd
from datetime import datetime

# Connect to SQLite database
conn = sqlite3.connect("transactions.db")
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        TransactionID TEXT UNIQUE,
        AccountID TEXT,
        TransactionAmount REAL,
        TransactionDate TEXT,
        TransactionType TEXT,
        TransactionHour INTEGER
    )
""")
conn.commit()

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    "bank-------transactions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Consume messages and insert into database
for msg in consumer:
    data = msg.value
    
    try:
        # Ensure all required keys exist
        required_keys = ["TransactionID", "AccountID", "TransactionAmount", "TransactionDate", "TransactionType"]
        if not all(key in data for key in required_keys):
            print(f"Skipping invalid transaction: {data}")
            continue
        
        # Extract transaction hour
        try:
            transaction_hour = datetime.strptime(data["TransactionDate"], "%Y-%m-%d %H:%M:%S").hour
        except ValueError:
            print(f"Skipping invalid date format: {data['TransactionDate']}")
            continue
        
        # Insert into SQLite database
        cursor.execute("""
            INSERT OR IGNORE INTO transactions 
            (TransactionID, AccountID, TransactionAmount, TransactionDate, TransactionType, TransactionHour) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, (data["TransactionID"], data["AccountID"], data["TransactionAmount"], data["TransactionDate"], data["TransactionType"], transaction_hour))
        
        conn.commit()
        print(f"Inserted: {data}")

    except Exception as e:
        print(f"Error processing transaction: {data} - {str(e)}")

conn.close()
