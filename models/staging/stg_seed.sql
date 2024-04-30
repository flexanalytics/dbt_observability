select
    command_invocation_id,
    node_id,
    run_started_at,
    database_name,
    schema_name,
    name,
    package_name,
    path,
    checksum
from {{ ref('seeds') }}
