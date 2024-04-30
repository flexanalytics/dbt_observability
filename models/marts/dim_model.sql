with model as (
    select
        command_invocation_id,
        node_id,
        database_name,
        schema_name,
        name,
        package_name,
        path,
        checksum,
        materialization,
        tags,
        meta,
        description
    from {{ ref('stg_model') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as model_key,
    node_id,
    database_name,
    schema_name,
    name,
    package_name,
    path,
    checksum,
    materialization,
    tags,
    meta,
    description
from model

/*
should dim_model just be models, or all invocations? ie dim_resource
with all resources and invocation history and then dim_model
with just node, name, database, schema, etc
*/
