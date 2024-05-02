with
    snapshots as (
        select
            command_invocation_id,
            node_id,
            database_name,
            schema_name,
            name,
            depends_on_nodes,
            package_name,
            path,
            checksum,
            strategy
        from {{ ref('stg_snapshot') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as snapshot_key,
    node_id,
    database_name,
    schema_name,
    name,
    depends_on_nodes,
    package_name,
    path,
    checksum,
    strategy
from snapshots
