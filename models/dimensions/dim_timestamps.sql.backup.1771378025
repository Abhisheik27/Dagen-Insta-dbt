{{
  config(
    materialized='table',
    schema='dimensions',
    tags=['dimensions', 'instagram'],
    description='Dimension table for timestamps with date and time attributes'
  )
}}

WITH stg_posts AS (
  SELECT DISTINCT
    CAST(created_at AS DATE) AS date_value
  FROM {{ ref('stg_insta_posts') }}
  WHERE created_at IS NOT NULL
),

date_spine AS (
  SELECT
    date_value,
    EXTRACT(YEAR FROM date_value) AS year,
    EXTRACT(MONTH FROM date_value) AS month,
    FORMAT_DATE('%B', date_value) AS month_name,
    EXTRACT(DAY FROM date_value) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_value) AS day_of_week,
    FORMAT_DATE('%A', date_value) AS day_name,
    EXTRACT(WEEK FROM date_value) AS week_of_year,
    EXTRACT(DAYOFYEAR FROM date_value) AS day_of_year,
    EXTRACT(QUARTER FROM date_value) AS quarter,
    MOD(EXTRACT(DAYOFWEEK FROM date_value), 7) IN (0, 1) AS is_weekend
  FROM stg_posts
),

dim_timestamps AS (
  SELECT
    {{ generate_surrogate_key(['date_value']) }} AS timestamp_sk,
    date_value AS date,
    year,
    month,
    month_name,
    day_of_month,
    day_of_week,
    day_name,
    week_of_year,
    day_of_year,
    quarter,
    is_weekend,
    CURRENT_TIMESTAMP() AS dim_loaded_at
  FROM date_spine
)

SELECT * FROM dim_timestamps