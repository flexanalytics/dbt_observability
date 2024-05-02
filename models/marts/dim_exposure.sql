with
    exposures as (
        select
            command_invocation_id,
            node_id,
            name,
            type,
            owner,
            maturity,
            path,
            description,
            url,
            package_name,
            depends_on_nodes
        from {{ ref('stg_exposure') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id'] ) }} as exposure_key,
    node_id,
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
