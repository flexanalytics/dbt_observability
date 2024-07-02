{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
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
