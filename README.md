# RealTime-Bank-Dashboard
A real-time dashboard using Kafka, SQLite, and Streamlit
# 🏦 Real-Time Bank Transactions Dashboard

A real-time dashboard to monitor bank transactions using Apache Kafka, SQLite, and Streamlit.

---

### 🚀 Features

- Real-time data streaming with Kafka  
- Transaction storage using SQLite  
- Live dashboard with auto-refresh (every 2 sec)  
- Interactive visualizations (Plotly + Streamlit)

---

### 🛠 Tech Stack

- Python  
- Apache Kafka  
- SQLite  
- Streamlit  
- Plotly  
- Pandas

---

### ⚙️ Setup

#### 1. Install Dependencies
```bash
pip install streamlit pandas plotly kafka-python
```

#### 2. Start Kafka

```bash
# Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Kafka Broker
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

#### 3. Create Topic

```bash
.\bin\windows\kafka-topics.bat --create --topic bank-transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### 4. Start Producer

```bash
python producer.py
```

#### 5. Start Consumer

```bash
python consumer.py
```

#### 6. Run Dashboard

```bash
streamlit run dashboard.py
```

---
