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
    freshness
from {{ ref('sources') }}
