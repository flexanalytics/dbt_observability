select
    command_invocation_id,
    node_id,
    run_started_at,
    name,
    depends_on_nodes,
    package_name,
    test_path,
    tags,
    test_metadata
from {{ ref('tests') }}
