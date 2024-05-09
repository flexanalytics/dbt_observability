with
    executions as (
        select
            command_invocation_id,
            node_id,
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
            schema_name,
            name,
            resource_type
        from {{ ref('stg_execution') }}
    )
select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id']) }} as invocation_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as model_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as column_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as test_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as seed_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as source_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as snapshot_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as metric_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as exposure_key,
    {{ dbt_utils.generate_surrogate_key(['run_start_day']) }} as date_key,
    run_started_at,
    was_full_refresh,
    thread_id,
    compile_started_at,
    query_completed_at,
    total_node_runtime
from executions
