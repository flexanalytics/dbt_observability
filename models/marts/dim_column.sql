with
    columns as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            column_name,
            data_type,
            tags,
            meta,
            description
        from {{ ref('int_column') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name', 'column_name']) }} as column_key,
    node_id,
    resource_type,
    project,
    resource_name,
    column_name,
    data_type,
    tags,
    meta,
    description
from columns
