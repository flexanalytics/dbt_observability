with
    test as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            name,
            depends_on_nodes,
            package_name,
            test_path,
            tags,
            test_metadata
        from {{ ref('int_test') }}
    )

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'resource_type',
        'project',
        'resource_name'
    ]) }} as test_key,
    node_id,
    resource_type,
    project,
    resource_name,
    name,
    depends_on_nodes,
    test_path,
    tags,
    test_metadata
from test
