select
    command_invocation_id,
    node_id,
    run_started_at,
    name,
    type,
    owner,
    maturity,
    path,
    description,
    url,
    package_name,
    depends_on_nodes
from {{ ref('exposures') }}
