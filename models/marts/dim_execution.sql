with executions as (
    select
        command_invocation_id,
        node_id,
        status,
        materialization,
        schema_name,
        name,
        resource_type
    from {{ ref('stg_execution') }}
)
select {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
    node_id,
    status,
    materialization,
    schema_name,
    name,
    resource_type
from executions
