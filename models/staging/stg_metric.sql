select
    command_invocation_id,
    unique_id,
    name,
    label,
    model,
    expression,
    calculation_method,
    filters,
    time_grains,
    dimensions,
    package_name,
    meta,
    depends_on_nodes,
    description
from {{ ref('metrics') }}
