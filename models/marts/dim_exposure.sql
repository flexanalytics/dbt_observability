with
    exposures as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            name,
            type,
            owner,
            maturity,
            path,
            description,
            url,
            package_name,
            depends_on_nodes
        from {{ ref('int_exposure') }}
    )

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'node_id'
    ] ) }} as exposure_key,
    node_id,
    resource_type,
    project,
    resource_name,
    name,
    type,
    owner,
    maturity,
    path,
    description,
    url,
    package_name,
    depends_on_nodes
from exposures
