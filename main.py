import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Load the data
df = pd.read_csv('./data1.csv', parse_dates=['date'])

st.header('Heat Stress At Work Warning at a glance 工作暑熱警告速覽')

# User selections for x-axis and y-axis
xaxis = st.radio("Sort By", ['Month', 'Hour'])
yaxis = st.radio("Method", ['Sum', 'Count'])

# Create a new column for x-axis values based on user selection
if xaxis == "Month":
    df['xaxis'] = df['date'].dt.month
    x_range = range(1,12)
elif xaxis == 'Hour':
    df['xaxis'] = df['date'].dt.hour
    x_range = range(0,23)

# Group by x-axis and apply the selected method
if yaxis == "Sum":
    df1 = df.groupby(['xaxis'])['duration'].sum()
    st.bar_chart(df1)
elif yaxis == 'Count':
    df1 = df.groupby(['xaxis'])['duration'].count()
    st.bar_chart(df1)