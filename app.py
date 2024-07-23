import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time

# Function to fetch data for the first KPI from MySQL using SQLAlchemy
def fetch_gender_data():
    # Define your MySQL database connection string
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    # Create SQLAlchemy engine
    engine = create_engine(db_connection_str)

    # SQL query to fetch data
    query = "SELECT Gender, count FROM gender_counts ORDER BY Gender"

    # Fetch data into a Pandas DataFrame
    df = pd.read_sql(query, engine)

    return df

# Function to fetch data for the second KPI from MySQL using SQLAlchemy
def fetch_class_data():
    # Define your MySQL database connection string
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    # Create SQLAlchemy engine
    engine = create_engine(db_connection_str)

    # SQL query to fetch data
    query = "SELECT Class, satisfaction, count FROM satisfaction_by_class ORDER BY Class, satisfaction"

    # Fetch data into a Pandas DataFrame
    df = pd.read_sql(query, engine)

    return df

# Main Streamlit app
def main():
    st.title('Real-time KPIs')

    # Fetch data from MySQL using SQLAlchemy for the first KPI
    gender_df = fetch_gender_data()

    # Ensure 'count' column is numeric (convert if necessary)
    gender_df['count'] = pd.to_numeric(gender_df['count'], errors='coerce')  # Convert to numeric, coerce errors

    # Plot bar chart of counts for the first KPI
    st.bar_chart(gender_df.set_index('Gender')['count'], use_container_width=True)

    # Fetch data from MySQL using SQLAlchemy for the second KPI
    class_df = fetch_class_data()

    # Ensure 'count' column is numeric (convert if necessary)
    class_df['count'] = pd.to_numeric(class_df['count'], errors='coerce')  # Convert to numeric, coerce errors

    # Pivot the data for better visualization
    pivot_df = class_df.pivot(index='Class', columns='satisfaction', values='count').fillna(0)

    # Plot bar chart for the second KPI
    st.bar_chart(pivot_df, use_container_width=True)

    time.sleep(2)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
