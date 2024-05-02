with
    source as (
        select
            command_invocation_id,
            node_id,
            database_name,
            schema_name,
            source_name,
            loader,
            name,
            identifier,
            loaded_at_field,
            freshness
        from {{ ref('stg_source') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as source_key,
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
