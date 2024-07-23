import streamlit as st
import plotly.figure_factory as ff
import plotly.graph_objects as go
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

def main():
    st.title('Real-time KPIs')

    gender_df = fetch_gender_data()
    gender_df['count'] = pd.to_numeric(gender_df['count'], errors='coerce')

    fig = go.Figure(
        data=[go.Pie(labels=gender_df['Gender'], values=gender_df['count'], hole=0.3)])
    fig.update_layout(title_text='Gender Distribution')
    st.plotly_chart(fig, use_container_width=True)

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

    st.plotly_chart(bar_fig, use_container_width=True)

    time.sleep(2)
    st.experimental_rerun()

if __name__ == '__main__':
    main()
