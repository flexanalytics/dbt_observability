with
    columns as (
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
        from {{ ref('stg_column') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id', 'column_name']) }} as column_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
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
from columns
