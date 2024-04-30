with test as (
    select
        command_invocation_id,
        node_id,
        name,
        depends_on_nodes,
        package_name,
        test_path,
        tags,
        test_metadata
    from {{ ref('stg_test') }}
)
select {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id'])}} as test_key,
    node_id,
    name,
    depends_on_nodes,
    package_name,
    test_path,
    tags,
    test_metadata
from test
