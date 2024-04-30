with seeds as (
    select
    command_invocation_id,
    node_id,
    database_name,
    schema_name,
    name,
    package_name,
    path,
    checksum
from {{ ref('stg_seed') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id'])}} as seed_key,
    node_id,
    database_name,
    schema_name,
    name,
    package_name,
    path,
    checksum
from seeds
