{% set end_date = run_started_at.strftime("%Y-%m-%d") %}

{{ dbt_date.get_date_dimension('2024-01-01', end_date) }}
