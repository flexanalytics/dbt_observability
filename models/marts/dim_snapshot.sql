with
    snapshots as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            database_name,
            schema_name,
            name,
            depends_on_nodes,
            package_name,
            path,
            checksum,
            strategy
        from {{ ref('int_snapshot') }}
    )

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'resource_type',
        'project',
        'resource_name',]
        ) }} as snapshot_key,
    node_id,
    resource_type,
    project,
    resource_name,
    database_name,
    schema_name,
    name,
    depends_on_nodes,
    path,
    checksum,
    strategy
from snapshots
