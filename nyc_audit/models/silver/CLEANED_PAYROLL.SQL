{{ config(
    materialized='incremental',
    unique_key='ID',
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY "agency_name") AS ID,
    "agency_name" AS AGENCY_NAME,
    "agency_start_date" AS AGENCY_START_DATE,
    "title_description" AS TITLE_DESCRIPTION,
    -- Convert hourly to annual, leave salary as is
    CASE
        WHEN "pay_basis" ILIKE '%hour%' THEN "base_salary" * 2080
        ELSE "base_salary"
    END AS ANNUALIZED_SALARY,
    "pay_basis" AS PAY_BASIS
FROM {{ source('NYC_JOBS', 'ANNUAL_PAYROLL') }}