{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    source as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            database_name,
            schema_name,
            source_name,
            loader,
            name,
            identifier,
            loaded_at_field,
            freshness
        from {{ ref('int_source') }}
    )

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'node_id'
    ]) }} as source_key,
    node_id,
    database_name,
    schema_name,
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness
from source
