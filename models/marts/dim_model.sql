with
    model as (
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
        from {{ ref('stg_model') }}
    ),

    _tests as (
        select lower(depends_on_nodes) as depends_on_nodes
        from {{ ref('stg_test') }}
    ),

    _models as (
        select
            '%' || lower(node_id) || '%' as node_key,
            node_id,
            model_key
        from model
    ),

    untested_models as (
        select distinct model.model_key
        from _models as model
        left outer join _tests as test
            on test.depends_on_nodes like model.node_key
        where test.depends_on_nodes is null
    ),

    final as (
        select model.*, case when untested_models.model_key is null then 1 else 0 end as is_tested from model left join untested_models on model.model_key = untested_models.model_key
    )

select * from final
