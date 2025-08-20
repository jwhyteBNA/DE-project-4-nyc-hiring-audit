{{ config(
    materialized='incremental',
    unique_key='ID',
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "job_id") AS ID,
    "agency" AS AGENCY,
    "business_title" AS BUSINESS_TITLE,
    "civil_service_title" AS CIVIL_SERVICE_TITLE,
    "job_category" AS JOB_CATEGORY,
    CASE
        WHEN "salary_frequency" ILIKE '%hour%' THEN "salary_range_from" * 2080
        ELSE "salary_range_from"
    END AS PAYROLL_SALARY_MIN_ANNUAL,
    CASE
        WHEN "salary_frequency" ILIKE '%hour%' THEN "salary_range_to" * 2080
        ELSE "salary_range_to"
    END AS PAYROLL_SALARY_MAX_ANNUAL,
    "salary_frequency" AS SALARY_FREQUENCY,
    TO_DATE("posting_date") AS POSTING_DATE_NORM,
    COALESCE(
        TO_DATE("post_until", 'DD-MON-YYYY'),
        CURRENT_DATE()
    ) AS POST_UNTIL_NORM
FROM {{ source('NYC_JOBS', 'WEEKLY_JOBS') }}