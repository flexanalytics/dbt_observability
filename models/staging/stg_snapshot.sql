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
    strategy
from {{ ref('snapshots') }}
