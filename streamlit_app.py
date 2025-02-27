import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.functions import col
from datetime import date

# Establish Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()

# User input for date range selection
st.title("Task Completion Bar Chart")

# Set default date range (Feb 1, 2025 - Today)
default_start = date(2025, 2, 1)
default_end = date.today()
min_start = date(2023, 1, 1)

start_date = st.date_input("Start Date", default_start, min_value=min_start, max_value=default_end)
end_date = st.date_input("End Date", default_end, min_value=start_date, max_value=default_end)

# Load tasks from Snowflake based on user-selected date range
@st.cache_data
def get_tasks(start, end):
    return (
        session.table("smartsheet.test.tasks")
        .select(col("TASK_NAME"), col("COMPLETION_DATE"))
        .filter((col("COMPLETION_DATE") >= str(start)) & (col("COMPLETION_DATE") <= str(end)))
        .to_pandas()
    )

# Fetch data with user-selected date range
df = get_tasks(start_date, end_date)

# Convert completion_date to date type
df['COMPLETION_DATE'] = pd.to_datetime(df['COMPLETION_DATE']).dt.date

# Group tasks by completion date
task_counts = df.groupby('COMPLETION_DATE').size().reset_index(name='Task Count')

# Show chart only if data is available
if not df.empty:
    # Create Altair Bar Chart
    chart = alt.Chart(task_counts).mark_bar().encode(
        x=alt.X('COMPLETION_DATE:T', title='Date'),
        y=alt.Y('Task Count:Q', title='Number of Completed Tasks'),
        tooltip=['COMPLETION_DATE', 'Task Count']
    ).properties(width=700, height=400)

    # Display Chart
    st.altair_chart(chart, use_container_width=True)

    # Optional: Show raw data
    if st.checkbox("Show Raw Data"):
        st.dataframe(df)
else:
    st.warning("No tasks found for the selected date range. Try a different range.")

