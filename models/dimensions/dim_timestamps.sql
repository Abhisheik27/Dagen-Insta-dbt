{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'instagram', 'time']
  )
}}

WITH date_spine AS (
  SELECT DISTINCT
    CAST(created_at AS DATE) AS date_value
  FROM {{ ref('stg_insta_posts') }}
  WHERE created_at IS NOT NULL
)

SELECT
  MD5(CAST(date_value AS STRING)) AS timestamp_sk,
  date_value AS date,
  EXTRACT(YEAR FROM date_value) AS year,
  EXTRACT(MONTH FROM date_value) AS month,
  FORMAT_DATE('%B', date_value) AS month_name,
  EXTRACT(DAY FROM date_value) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM date_value) AS day_of_week,
  FORMAT_DATE('%A', date_value) AS day_name,
  EXTRACT(WEEK FROM date_value) AS week_of_year,
  EXTRACT(DAYOFYEAR FROM date_value) AS day_of_year,
  EXTRACT(QUARTER FROM date_value) AS quarter,
  CASE WHEN EXTRACT(DAYOFWEEK FROM date_value) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
  CURRENT_TIMESTAMP() AS dim_loaded_at
FROM date_spine
ORDER BY date_value