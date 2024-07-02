{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    column_name,
    data_type,
    tags,
    meta,
    description,
    is_documented,
    row_count,
    row_distinct,
    row_null,
    row_min,
    row_max,
    row_avg,
    row_sum,
    row_stdev,
    is_metric,
    column_values
from {{ ref('columns') }}
