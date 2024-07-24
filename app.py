import streamlit as st
import plotly.figure_factory as ff
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from sqlalchemy import create_engine
import time


def fetch_gender_data():
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    engine = create_engine(db_connection_str)
    query = "SELECT Gender, count FROM gender_counts ORDER BY Gender"
    df = pd.read_sql(query, engine)

    return df


def fetch_class_data():
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    engine = create_engine(db_connection_str)
    query = "SELECT Class, satisfaction, count FROM satisfaction_by_class ORDER BY Class, satisfaction"
    df = pd.read_sql(query, engine)

    return df


def fetch_age_data():
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    engine = create_engine(db_connection_str)
    query = "SELECT Age, count FROM age_distribution ORDER BY Age"
    df = pd.read_sql(query, engine)

    return df


def fetch_type_of_travel_data():
    db_connection_str = 'mysql+pymysql://root:12345678@localhost:3306/spark_streaming_db'

    engine = create_engine(db_connection_str)
    query = "SELECT Type_of_Travel, count FROM type_travel_counts ORDER BY Type_of_Travel"
    df = pd.read_sql(query, engine)

    return df

def main():
    st.title('Real-time KPIs')

    gender_df = fetch_gender_data()
    gender_df['count'] = pd.to_numeric(gender_df['count'], errors='coerce')

    fig1 = go.Figure(
        data=[go.Pie(labels=gender_df['Gender'], values=gender_df['count'], hole=0.3)])
    fig1.update_layout(title_text='Gender Distribution')


    class_df = fetch_class_data()
    class_df['count'] = pd.to_numeric(class_df['count'], errors='coerce')
    pivot_df = class_df.pivot(
        index='Class', columns='satisfaction', values='count').fillna(0)

    bar_fig = go.Figure()
    for satisfaction in pivot_df.columns:
        bar_fig.add_trace(
            go.Bar(
                x=pivot_df.index,
                y=pivot_df[satisfaction],
                name=f'Satisfaction {satisfaction}'
            )
        )

    bar_fig.update_layout(
        title_text='Satisfaction by Class',
        xaxis_title='Class',
        yaxis_title='Count',
        barmode='stack'
    )

    age_df = fetch_age_data()
    age_df['count'] = pd.to_numeric(age_df['count'], errors='coerce')

    age_hist = px.histogram(age_df, x='Age', y='count', title='Age Distribution',
                            labels={'Age': 'Age', 'count': 'Count'},
                            nbins=20)
    st.plotly_chart(age_hist, use_container_width=True)

    travel_df = fetch_type_of_travel_data()
    travel_df['count'] = pd.to_numeric(travel_df['count'], errors='coerce')

    fig3 = go.Figure(
        data=[go.Bar(x=travel_df['Type_of_Travel'], y=travel_df['count'])]
    )
    fig3.update_layout(title_text='Type of Travel Counts')

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.plotly_chart(bar_fig, use_container_width=True)

    st.plotly_chart(fig3, use_container_width=True)

    time.sleep(5)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
