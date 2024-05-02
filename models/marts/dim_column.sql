with
    columns as (
        select
            command_invocation_id,
            node_id,
            column_name,
            data_type,
            tags,
            meta,
            description
        from {{ ref('stg_column') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id', 'column_name']) }} as column_key,
    node_id,
    column_name,
    data_type,
    tags,
    meta,
    description
from columns
