{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    run_started_at,
    was_full_refresh,
    thread_id,
    status,
    compile_started_at,
    query_completed_at,
    total_node_runtime,
    materialization,
    schema_name,
    name,
    resource_type
from {{ ref('all_executions') }}
