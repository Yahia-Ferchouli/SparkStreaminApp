import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time

# Function to fetch data from MySQL using SQLAlchemy
def fetch_data():
    # Define your MySQL database connection string
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    # Create SQLAlchemy engine
    engine = create_engine(db_connection_str)

    # SQL query to fetch data
    query = "SELECT Gender, count FROM gender_counts ORDER BY Gender"

    # Fetch data into a Pandas DataFrame
    df = pd.read_sql(query, engine)

    return df

# Main Streamlit app
def main():
    st.title('Real-time KPIs')

    # Fetch data from MySQL using SQLAlchemy
    df = fetch_data()

    # Ensure 'count' column is numeric (convert if necessary)
    df['count'] = pd.to_numeric(df['count'],
                                errors='coerce')  # Convert to numeric, coerce errors

    # Display the latest accumulated data in a table
    st.write("Latest Accumulated Data:")
    st.write(df)

    df = fetch_data()



    # Filter dataframe to only include 'Male' and 'Female'
    df_filtered = df[df['Gender'].isin(['Male', 'Female'])]

    # Plot bar chart of counts
    st.bar_chart(df_filtered.set_index('Gender')['count'],
                 use_container_width=True)


    time.sleep(1)
    st.rerun()

if __name__ == '__main__':
    main()
