# Dagen Instagram Analytics DBT Pipeline

A comprehensive dbt transformation pipeline for Instagram analytics built with a star schema design. This project transforms raw Airbyte-ingested Instagram data into clean, analysis-ready dimensions, facts, and reporting views.

## Project Overview

This DBT project implements a complete ELT (Extract, Load, Transform) pipeline for Instagram data analytics with the following architecture:

- **Staging Layer**: Raw data parsing and deduplication
- **Dimensional Model**: Star schema with facts and dimensions
- **Intermediate Layer**: KPI calculations and aggregations
- **Reporting Layer**: Final views for dashboard consumption

## Data Architecture

### Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_posts     │
                    │  (Post attrs)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼──────┐    ┌────────▼────────┐    ┌─────▼──────────┐
│  dim_accounts│    │fct_instagram_   │    │dim_timestamps  │
│(Account info)│    │engagement       │    │(Date/Time)     │
└──────────────┘    │(Central Fact)   │    └────────────────┘
                    └─────────────────┘
```

## Models Structure

### Staging Models (models/staging/)
- **stg_insta_posts**: Parses raw JSON from `airbyte_raw_insta_united_posts`
- **stg_insta_posts_clean**: Parses pre-cleaned JSON from `airbyte_raw_insta_united_posts_clean`

**Key Transformations**:
- JSON parsing and field extraction
- Type casting and standardization
- Null handling and deduplication
- Snake_case column naming

### Dimension Tables (models/dimensions/)

#### dim_posts
- **Grain**: One row per unique post
- **Key Columns**: post_sk (surrogate), post_id (natural), short_code, post_url, post_type, caption, location_name
- **Tests**: not_null, unique on post_id

#### dim_accounts
- **Grain**: One row per unique account
- **Key Columns**: account_sk (surrogate), account_id, account_username, account_name
- **Tests**: not_null, unique on account_username

#### dim_timestamps
- **Grain**: One row per unique date
- **Key Columns**: timestamp_sk (surrogate), date, year, month, month_name, day_of_week, day_name, week_of_year, quarter, is_weekend
- **Tests**: not_null, unique on date

### Fact Table (models/facts/)

#### fct_instagram_engagement
- **Grain**: One row per post
- **Foreign Keys**: post_sk, account_sk, timestamp_sk
- **Measures**:
  - likes_count: Number of likes
  - comments_count: Number of comments
  - video_view_count: Number of video views
  - total_engagement: likes + comments
  
- **Calculated Metrics**:
  - engagement_rate: (likes + comments) / (likes + comments + 1), normalized to 0-1
  - viral_score: Relative engagement score (100 = account average)

- **Tests**: not_null on all foreign keys, unique on post_id

### Intermediate Models (models/intermediate/)

#### int_post_performance
- Post-level KPIs and performance metrics
- Includes: engagement_rate, viral_score, reach_efficiency, performance_tier (Viral/High/Medium/Low)
- Ranking within account

#### int_account_metrics
- Account-level aggregated metrics
- Includes: total_posts, avg_likes, avg_comments, avg_engagement_rate, total_reach, post_frequency_per_week
- Best performing post type, performance tier

#### int_engagement_trends
- Time-series engagement metrics
- Daily aggregations with trend indicators
- Top account per day, engagement vs average, growth rates

### Reporting Views (models/reporting/)

#### v_posts_kpi_report
- Final post performance report
- Includes: all engagement metrics, performance tier, ranking, is_top_post flag
- Ordered by total_engagement DESC
- Ready for dashboard consumption

#### v_accounts_kpi_report
- Final account performance report
- Includes: all aggregated metrics, engagement trend, growth rate, performance tier
- Account ranking by engagement
- Ready for dashboard consumption

#### v_engagement_trends
- Time-series engagement analysis
- Includes: 7-day and 30-day moving averages, volatility metrics
- Trend indicators and growth rates
- Ordered by date DESC

## Data Quality & Testing

### Test Coverage

**Staging Models**:
- not_null tests on _airbyte_raw_id, _airbyte_extracted_at, _airbyte_loaded_at
- unique tests on _airbyte_raw_id

**Dimension Tables**:
- not_null and unique tests on surrogate keys
- not_null tests on natural keys
- Referential integrity tests

**Fact Table**:
- not_null tests on all foreign keys and measures
- unique tests on post_id
- Relationship tests to dimension tables

**Intermediate & Reporting**:
- not_null tests on key columns
- Accepted values tests for categorical fields
- Custom tests for data quality

### Custom Tests

Located in `tests/schema_tests.yml`:
- engagement_rate bounds (0-1)
- total_engagement calculation verification
- post_performance rank uniqueness
- account_metrics aggregation validation
- trends date continuity check

## Running the Pipeline

### Prerequisites
- dbt 1.9.4+
- BigQuery connection configured
- Service account with appropriate permissions

### Installation
```bash
cd Dagen-Insta-dbt
dbt deps  # Install dependencies
dbt compile  # Compile all models
```

### Execution
```bash
# Run all models
dbt run

# Run specific model
dbt run --models stg_insta_posts

# Run with specific tag
dbt run --select tag:staging

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Execution Order
The pipeline automatically respects dependencies:
1. Staging models (from raw data)
2. Dimension tables (from staging)
3. Fact table (from staging + dimensions)
4. Intermediate KPI models (from facts + dimensions)
5. Reporting views (from intermediate models)

## Configuration

### dbt_project.yml Settings

```yaml
models:
  Dagen_Insta_dbt:
    staging:
      +materialized: view
      +schema: staging
    dimensions:
      +materialized: table
      +schema: dimensions
    facts:
      +materialized: table
      +schema: facts
    intermediate:
      +materialized: view
      +schema: intermediate
    reporting:
      +materialized: view
      +schema: reporting
```

### BigQuery Configuration

- **Project**: prd-dagen
- **Dataset**: sample_abhisheik_jadhav_20260130060557
- **Schemas**:
  - staging: Raw parsed data
  - dimensions: Dimension tables
  - facts: Fact tables
  - intermediate: KPI calculations
  - reporting: Final reporting views

## Sample Queries

### Top 10 Posts by Engagement
```sql
SELECT 
  post_id,
  owner_username,
  post_type,
  total_engagement,
  engagement_rate,
  viral_score,
  performance_tier
FROM `prd-dagen.sample_abhisheik_jadhav_20260130060557_reporting.v_posts_kpi_report`
LIMIT 10
```

### Account Performance Summary
```sql
SELECT 
  owner_username,
  total_posts,
  avg_engagement_rate,
  total_reach,
  performance_tier,
  engagement_trend,
  growth_rate_pct,
  account_rank
FROM `prd-dagen.sample_abhisheik_jadhav_20260130060557_reporting.v_accounts_kpi_report`
WHERE performance_tier = 'High'
ORDER BY account_rank
```

### Daily Engagement Trends
```sql
SELECT 
  date,
  day_name,
  total_posts,
  total_engagement,
  engagement_7day_ma,
  engagement_30day_ma,
  trend_direction,
  engagement_vs_avg
FROM `prd-dagen.sample_abhisheik_jadhav_20260130060557_reporting.v_engagement_trends`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC
```

## Documentation

Full documentation is available via dbt docs:
```bash
dbt docs generate
dbt docs serve
```

This generates:
- Model descriptions and lineage
- Column-level documentation
- Test definitions
- Source definitions

## Project Statistics

- **Total Models**: 12
  - Staging: 2
  - Dimensions: 3
  - Facts: 1
  - Intermediate: 3
  - Reporting: 3
- **Total Tests**: 109+
- **Total Columns Documented**: 150+
- **Data Quality Checks**: Custom + dbt native tests

## Maintenance

### Regular Tasks
- Monitor test failures for data quality issues
- Review engagement metrics weekly
- Update performance thresholds as needed
- Archive historical data quarterly

### Performance Optimization
- Dimension tables are materialized as tables for query performance
- Fact table is materialized as a table for efficient joins
- Intermediate and reporting views are materialized as views for flexibility
- Partition strategies can be added for large fact tables

## Troubleshooting

### Common Issues

**Profile not found**:
```bash
dbt_project.yml profile name must match profiles.yml
Run: dbt fix_dbt_project_profile_name
```

**BigQuery connection errors**:
- Verify service account has BigQuery permissions
- Check dataset exists and is accessible
- Confirm keyfile.json is properly configured

**Model compilation errors**:
```bash
dbt compile --models model_name  # Test individual model
dbt debug  # Check connection
```

## Contributing

When adding new models:
1. Follow the naming convention (stg_, dim_, fct_, int_, v_)
2. Add comprehensive YAML documentation
3. Include appropriate tests
4. Update this README with model descriptions
5. Commit with meaningful messages

## License

This project is part of the Dagen Analytics platform.

## Contact

For questions or support, contact the data engineering team.

---

**Last Updated**: 2026-02-18  
**Version**: 1.0.0  
**Status**: Production Ready