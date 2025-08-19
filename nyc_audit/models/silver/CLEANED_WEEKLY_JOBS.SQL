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
    "salary_range_from" AS PAYROLL_SALARY_MIN,
    "salary_range_to" AS PAYROLL_SALARY_MAX,
    "salary_frequency" AS SALARY_FREQUENCY,
    TO_DATE("posting_date") AS POSTING_DATE_NORM,
    COALESCE(
        TO_DATE("post_until", 'DD-MON-YYYY'),
        DATEADD(day, 30, TO_DATE("posting_date"))
    ) AS POST_UNTIL_NORM
FROM {{ source('NYC_JOBS', 'WEEKLY_JOBS') }}