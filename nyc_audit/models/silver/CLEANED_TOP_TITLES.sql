{{ config(
    materialized='incremental',
    unique_key='ID',
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "Job Title") AS ID,
    "Job Title" AS JOB_TITLE,
    "Total Postings (Jan 2024 - Jun 2025)" AS TOTAL_POSTINGS,
    "Unique Postings (Jan 2024 - Jun 2025)" AS UNIQUE_POSTINGS,
    CAST(REGEXP_REPLACE("Median Posting Duration", ' days', '') AS INTEGER) AS MEDIAN_POSTING_DURATION_DAYS
FROM {{ source('NYC_JOBS', 'TOP_POSTED_TITLES') }}