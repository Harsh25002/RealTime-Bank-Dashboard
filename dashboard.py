import streamlit as st
import sqlite3
import pandas as pd
import time
import plotly.express as px

st.set_page_config(page_title="Bank Transactions Dashboard", layout="wide")
st.title("📊 Real-Time Bank Transactions Dashboard")

# Connect to SQLite
DB_PATH = "transactions.db"

# Fetch Data
def fetch_data():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM transactions ORDER BY id DESC LIMIT 100"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Placeholder for real-time updates
placeholder = st.empty()

# Load data once per run
df = fetch_data()

with placeholder.container():
    col1, col2 = st.columns(2)

    # 1️⃣ Recent Transactions
    with col1:
        st.subheader("📌 Recent Transactions")
        st.dataframe(df[['TransactionID', 'AccountID', 'TransactionAmount', 'TransactionDate', 'TransactionType']])

    # 2️⃣ Total Transaction Volume
    with col2:
        total_amount = df["TransactionAmount"].sum()
        st.subheader("💰 Total Transaction Volume")
        st.metric(label="Total Volume", value=f"${total_amount:.2f}")

    col3, col4 = st.columns(2)

    # 3️⃣ Transaction Type Distribution
    with col3:
        st.subheader("📊 Transaction Type Distribution")
        if not df.empty:
            type_counts = df["TransactionType"].value_counts().reset_index()
            type_counts.columns = ["TransactionType", "Count"]
            fig1 = px.bar(type_counts, x="TransactionType", y="Count", title="Transaction Type Frequency", color="TransactionType")
            st.plotly_chart(fig1, use_container_width=True)

    # 4️⃣ Account with Highest Transactions
    with col4:
        st.subheader("🏆 Account with Most Transactions")
        top_account = df["AccountID"].mode()[0] if not df["AccountID"].empty else "N/A"
        st.metric(label="Top Account", value=top_account)

    col5, col6 = st.columns(2)

    # 5️⃣ Top 5 Largest Transactions
    with col5:
        st.subheader("💵 Top 5 Largest Transactions")
        top_transactions = df.nlargest(5, "TransactionAmount")
        st.dataframe(top_transactions[['TransactionID', 'AccountID', 'TransactionAmount', 'TransactionDate', 'TransactionType']])

    # 6️⃣ Average Transaction Amount
    with col6:
        avg_amount = df["TransactionAmount"].mean()
        st.subheader("📉 Average Transaction Amount")
        st.metric(label="Average Amount", value=f"${avg_amount:.2f}")

    # 7️⃣ Hourly Transaction Trend
    st.subheader("⏳ Hourly Transaction Trend")
    if not df.empty:
        df["TransactionHour"] = pd.to_datetime(df["TransactionDate"]).dt.hour
        hourly_trend = df.groupby("TransactionHour").size().reset_index(name="TransactionCount")
        fig2 = px.line(hourly_trend, x="TransactionHour", y="TransactionCount", markers=True, title="Hourly Transactions", line_shape="spline")
        st.plotly_chart(fig2, use_container_width=True)

# 🔁 Refresh every 2 seconds
time.sleep(2)
st.rerun()





