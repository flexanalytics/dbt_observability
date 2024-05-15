with
    executions as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            run_started_at,
            cast(
                {{
                    dbt.date_trunc(
                        'day', 'run_started_at'
                        )
                    }} as date
            ) as run_start_day,
            was_full_refresh,
            thread_id,
            status,
            compile_started_at,
            query_completed_at,
            total_node_runtime,
            materialization,
            schema_name
        from {{ ref('int_execution') }}
    )
select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as execution_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id']) }} as invocation_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as model_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as column_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as test_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as seed_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as source_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as snapshot_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as metric_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'resource_type', 'project', 'resource_name']) }} as exposure_key,
    {{ dbt_utils.generate_surrogate_key(['run_start_day']) }} as date_key,
    run_started_at,
    was_full_refresh,
    thread_id,
    compile_started_at,
    query_completed_at,
    total_node_runtime
from executions
