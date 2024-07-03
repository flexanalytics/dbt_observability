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
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness,
    total_rowcount
from {{ ref('sources') }}
