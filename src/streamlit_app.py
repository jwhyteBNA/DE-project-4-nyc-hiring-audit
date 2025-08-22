import os
import pandas as pd
import altair as alt
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "schema": os.getenv("SNOWFLAKE_LOGIC_SCHEMA")
}

def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def load_gold_table(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM NYC_JOB_POSTINGS_PAYROLL_MATCHES")
    df = cursor.fetch_pandas_all()
    cursor.close()
    return df

st.title("NYC Job Postings Payroll Matches")

conn = get_connection()
gold_job_titles = load_gold_table(conn)
gold_job_titles_filtered = gold_job_titles[gold_job_titles["MATCH_SCORE"] >= 85]


if "MATCH_SCORE" in gold_job_titles_filtered.columns:
    st.subheader("Number of Postings by Match Score (85–100)")
    curve_data = gold_job_titles_filtered.copy()
    curve_data["MATCH_SCORE_INT"] = curve_data["MATCH_SCORE"].round().astype(int)
    score_counts = (
        curve_data[curve_data["MATCH_SCORE_INT"].between(85, 100)]
        .groupby("MATCH_SCORE_INT")["POSTING_ID"]
        .count()
        .reset_index()
        .rename(columns={"POSTING_ID": "Posting Count"})
    )
    chart = alt.Chart(score_counts).mark_bar().encode(
        x=alt.X("MATCH_SCORE_INT:O", title="Match Score"),
        y=alt.Y("Posting Count:Q", title="Number of Postings"),
        tooltip=[
            alt.Tooltip("MATCH_SCORE_INT", title="Match Score"),
            alt.Tooltip("Posting Count", title="Number of Postings")
        ]
    )
    st.altair_chart(chart, use_container_width=True)

if "MATCH_SCORE" in gold_job_titles_filtered.columns and "BUSINESS_TITLE" in gold_job_titles_filtered.columns:
    st.subheader("Match Score by Business Title (85–100)")
    table_data = gold_job_titles_filtered[["BUSINESS_TITLE", "PAYROLL_MATCHED_TITLE", "MATCH_SCORE", "POSTING_DURATION", "PAYROLL_SALARY_MIN_ANNUAL", "PAYROLL_SALARY_MAX_ANNUAL", "PAYROLL_ANNUALIZED_SALARY"]].copy()
    table_data = table_data[table_data["MATCH_SCORE"] >= 85].sort_values(
        ["MATCH_SCORE", "BUSINESS_TITLE"], ascending=[False, True]
    )
    st.dataframe(table_data, use_container_width=True)



if "MATCH_SCORE" in gold_job_titles_filtered.columns and "PAYROLL_ANNUALIZED_SALARY" in gold_job_titles_filtered.columns:
    st.subheader("Average Salary by Match Score (85–100)")
    scatter_data = gold_job_titles_filtered.copy()
    scatter_data["MATCH_SCORE_INT"] = scatter_data["MATCH_SCORE"].round().astype(int)
    avg_salary_by_score = (
        scatter_data[scatter_data["MATCH_SCORE_INT"].between(85, 100)]
        .groupby("MATCH_SCORE_INT")["PAYROLL_ANNUALIZED_SALARY"]
        .mean()
        .reset_index()
    )
    bar_chart = alt.Chart(avg_salary_by_score).mark_bar().encode(
        x=alt.X("MATCH_SCORE_INT:O", title="Match Score"),
        y=alt.Y("PAYROLL_ANNUALIZED_SALARY:Q", title="Average Annualized Salary"),
        tooltip=[
            alt.Tooltip("MATCH_SCORE_INT", title="Match Score"),
            alt.Tooltip("PAYROLL_ANNUALIZED_SALARY", title="Avg Salary")
        ]
    )
    st.altair_chart(bar_chart, use_container_width=True)

st.write(scatter_data[["MATCH_SCORE", "PAYROLL_ANNUALIZED_SALARY"]].describe())

if "MATCH_SCORE" in gold_job_titles_filtered.columns and "PAYROLL_ANNUALIZED_SALARY" in gold_job_titles_filtered.columns:
    st.subheader("Match Score vs. Payroll Annualized Salary (Scatter Chart)")
    scatter_data = gold_job_titles_filtered.copy()
    scatter_data = scatter_data.dropna(subset=["MATCH_SCORE", "PAYROLL_ANNUALIZED_SALARY"])
    st.scatter_chart(
        scatter_data[["MATCH_SCORE", "PAYROLL_ANNUALIZED_SALARY"]].sort_values("MATCH_SCORE")
    )


if "PAYROLL_MATCHED_TITLE" in gold_job_titles_filtered.columns:
    st.subheader("Top Matched Payroll Titles")
    top_titles = gold_job_titles_filtered["PAYROLL_MATCHED_TITLE"].value_counts().head(10)
    st.bar_chart(top_titles)


if (
    "PAYROLL_SALARY_MIN_ANNUAL" in gold_job_titles_filtered.columns
    and "PAYROLL_SALARY_MAX_ANNUAL" in gold_job_titles_filtered.columns
    and "PAYROLL_MATCHED_TITLE" in gold_job_titles_filtered.columns
):
    st.subheader("Payroll Salary Range Distribution (with Title Tooltip)")
    salary_data = gold_job_titles_filtered.dropna(subset=["PAYROLL_SALARY_MIN_ANNUAL", "PAYROLL_SALARY_MAX_ANNUAL", "PAYROLL_MATCHED_TITLE"])
    melted = salary_data.melt(
        id_vars=["PAYROLL_MATCHED_TITLE"],
        value_vars=["PAYROLL_SALARY_MIN_ANNUAL", "PAYROLL_SALARY_MAX_ANNUAL"],
        var_name="Salary Type",
        value_name="Salary"
    )
    line_chart = alt.Chart(melted).mark_line(point=True).encode(
        x=alt.X("PAYROLL_MATCHED_TITLE:N", title="Payroll Matched Title", sort=None),
        y=alt.Y(
        "Salary:Q",
        title="Salary",
        scale=alt.Scale(domain=[0, 300000]),  # Adjust domain as needed
        axis=alt.Axis(
            tickCount=7,  # Suggests more ticks
            values=[0, 50000, 100000, 150000, 200000, 250000, 300000]  # Explicit tick values
        )
    ),
        color=alt.Color(
            "Salary Type:N",
            title="Salary Type",
            legend=alt.Legend(orient="top")  # Move legend to top
        ),
        tooltip=[
            alt.Tooltip("PAYROLL_MATCHED_TITLE", title="Title"),
            alt.Tooltip("Salary Type", title="Type"),
            alt.Tooltip("Salary", title="Salary")
        ]
    ).properties(width=800, height=500)
    st.altair_chart(line_chart, use_container_width=True)


if "POSTING_DURATION" in gold_job_titles_filtered.columns:
    st.subheader("Distribution of Posting Duration (Days) - Streamlit Built-in")
    posting_duration_data = pd.to_numeric(gold_job_titles_filtered["POSTING_DURATION"], errors="coerce")
    posting_duration_data = posting_duration_data[
        posting_duration_data.notna() &
        (posting_duration_data >= 0)
    ]
    st.bar_chart(posting_duration_data)

if "POSTING_DURATION" in gold_job_titles_filtered.columns and "PAYROLL_SALARY_MIN_ANNUAL" in gold_job_titles_filtered.columns:
    st.subheader("Average Min Annual Salary by Posting Duration Bin")
    scatter_data = gold_job_titles_filtered.copy()
    scatter_data["POSTING_DURATION"] = pd.to_numeric(scatter_data["POSTING_DURATION"], errors="coerce")
    scatter_data["PAYROLL_SALARY_MIN_ANNUAL"] = pd.to_numeric(scatter_data["PAYROLL_SALARY_MIN_ANNUAL"], errors="coerce")
    scatter_data = scatter_data[
        scatter_data["POSTING_DURATION"].notna() &
        scatter_data["PAYROLL_SALARY_MIN_ANNUAL"].notna() &
        (scatter_data["POSTING_DURATION"] >= 0) &
        (scatter_data["PAYROLL_SALARY_MIN_ANNUAL"] > 0)
    ]

    scatter_data["DURATION_BIN"] = pd.cut(
        scatter_data["POSTING_DURATION"], bins=range(0, 101, 10), right=False
    )
    # Convert interval bins to custom strings for better display
    scatter_data["DURATION_BIN_STR"] = scatter_data["DURATION_BIN"].apply(
        lambda x: f"{int(x.left)}-{int(x.right)-1}" if pd.notnull(x) else "Unknown"
    )
    avg_salary_by_bin = scatter_data.groupby("DURATION_BIN_STR")["PAYROLL_SALARY_MIN_ANNUAL"].mean().reset_index()
    bar_chart = alt.Chart(avg_salary_by_bin).mark_bar().encode(
        x=alt.X("DURATION_BIN_STR:N", title="Posting Duration (Days, Binned)"),
        y=alt.Y("PAYROLL_SALARY_MIN_ANNUAL:Q", title="Average Min Annual Salary"),
        tooltip=[
            alt.Tooltip("DURATION_BIN_STR:N", title="Duration Bin"),
            alt.Tooltip("PAYROLL_SALARY_MIN_ANNUAL:Q", title="Avg Min Salary")
        ]
    ).properties(width=700, height=400)
    st.altair_chart(bar_chart, use_container_width=True)
    # ...existing code...


if "POSTING_DURATION" in gold_job_titles_filtered.columns and "PAYROLL_MATCHED_TITLE" in gold_job_titles_filtered.columns:
    st.subheader("Average Posting Duration per Payroll Matched Title")
    avg_duration_by_title = (
        gold_job_titles_filtered
        .groupby("PAYROLL_MATCHED_TITLE")["POSTING_DURATION"]
        .mean()
        .reset_index()
        .sort_values("POSTING_DURATION", ascending=False)
        .head(20)  # Show top 20 titles by average duration
    )
    bar_chart = alt.Chart(avg_duration_by_title).mark_bar().encode(
        y=alt.Y("PAYROLL_MATCHED_TITLE:N", sort="-x", title="Payroll Matched Title"),
        x=alt.X("POSTING_DURATION:Q", title="Average Posting Duration (Days)"),
        tooltip=[
            alt.Tooltip("PAYROLL_MATCHED_TITLE", title="Title"),
            alt.Tooltip("POSTING_DURATION", title="Avg Duration (Days)")
        ]
    ).properties(height=600)
    st.altair_chart(bar_chart, use_container_width=True)


if "POSTING_DURATION" in gold_job_titles_filtered.columns and "PAYROLL_MATCHED_TITLE" in gold_job_titles_filtered.columns:
    st.subheader("Median Posting Duration per Payroll Matched Title")
    median_duration_by_title = (
        gold_job_titles_filtered
        .groupby("PAYROLL_MATCHED_TITLE")["POSTING_DURATION"]
        .median()
        .reset_index()
        .sort_values("POSTING_DURATION", ascending=False)
        .head(20)  # Show top 20 titles by median duration
    )
    bar_chart = alt.Chart(median_duration_by_title).mark_bar().encode(
        y=alt.Y("PAYROLL_MATCHED_TITLE:N", sort="-x", title="Payroll Matched Title"),
        x=alt.X("POSTING_DURATION:Q", title="Median Posting Duration (Days)"),
        tooltip=[
            alt.Tooltip("PAYROLL_MATCHED_TITLE", title="Title"),
            alt.Tooltip("POSTING_DURATION", title="Median Duration (Days)")
        ]
    ).properties(height=600)
    st.altair_chart(bar_chart, use_container_width=True)