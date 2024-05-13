with
    executions as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            status,
            materialization,
            schema_name,
            run_started_at
        from {{ ref('int_execution') }}
    ),

    most_recent as (
        select max(run_started_at) as most_recent_run
        from executions
    )

select
    {{ dbt_utils.generate_surrogate_key(['executions.command_invocation_id', 'executions.resource_type', 'executions.project', 'executions.resource_name']) }} as execution_key,
    executions.node_id,
    executions.resource_type,
    executions.project,
    executions.resource_name,
    executions.status,
    executions.materialization,
    executions.schema_name,
    case when executions.run_started_at = most_recent.most_recent_run then 'Yes' else 'No' end as most_recent_run
from executions left join most_recent on executions.run_started_at = most_recent.most_recent_run
