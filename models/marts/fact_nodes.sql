with sources as (
    select
        command_invocation_id,
        node_id,
        run_started_at,
        database_name,
        schema_name,
        source_name,
        loader,
        name,
        identifier,
        loaded_at_field,
        freshness
    from {{ ref('stg_source') }}
),
seeds as (
    select
        command_invocation_id,
        node_id,
        run_started_at,
        database_name,
        schema_name,
        name,
        package_name,
        path,
        checksum
    from {{ ref('stg_seed') }}
),
models as (
    select
        command_invocation_id,
        node_id,
        run_started_at,
        database_name,
        schema_name,
        name,
        depends_on_nodes,
        package_name,
        path,
        checksum,
        materialization,
        tags,
        meta,
        description,
        total_rowcount
    from {{ ref('stg_model') }}
)

select 1
-- hoping to create foreign keys between
-- `depends_on_nodes` from models and the sources/seeds
