with
    executions as (
        select
            command_invocation_id,
            node_id,
            status,
            materialization,
            schema_name,
            name,
            run_started_at,
            resource_type
        from {{ ref('stg_execution') }}
    ),

    most_recent as (
        select max(run_started_at) as most_recent_run
        from executions
    )

select
    {{ dbt_utils.generate_surrogate_key(['executions.command_invocation_id', 'executions.node_id']) }} as execution_key,
    executions.node_id,
    executions.status,
    executions.materialization,
    executions.schema_name,
    executions.name,
    executions.resource_type,
    case when executions.run_started_at = most_recent.most_recent_run then 1 else 0 end as most_recent_run
from executions left join most_recent on executions.run_started_at = most_recent.most_recent_run
