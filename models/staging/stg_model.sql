{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    run_started_at,
    database_name,
    schema_name,
    name,
    depends_on_nodes,
    package_name,
    path,
    checksum,
    materialization,
    tags,
    meta,
    description,
    total_rowcount
from {{ ref('models') }}
