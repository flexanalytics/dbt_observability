{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    models as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
                ]) }} as model_key,
            node_id,
            resource_type,
            project,
            resource_name,
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
        from {{ ref('int_model') }}
    ),

    _tests as (
        select lower(
            {% if target.type == 'snowflake' %}
                array_to_string(depends_on_nodes, '')
            {% else %}
                depends_on_nodes
            {% endif %})
            as depends_on_nodes
        from {{ ref('stg_test') }}
    ),

    _models as (
        select
            '%' || lower(node_id) || '%' as node_key,
            node_id,
            model_key
        from models
    ),

    untested_models as (
        select distinct models.model_key
        from _models as models
        left outer join _tests as test
            on test.depends_on_nodes like models.node_key
        where test.depends_on_nodes is null
    ),

    final as (
        select
            models.model_key,
            models.node_id,
            models.resource_type,
            models.project,
            models.resource_name,
            models.database_name,
            models.schema_name,
            models.name,
            models.package_name,
            models.path,
            models.checksum,
            models.materialization,
            models.tags,
            models.meta,
            models.description,
            case when untested_models.model_key is null then 'Yes' else 'No' end as is_tested
        from models
        left outer join untested_models on models.model_key = untested_models.model_key
    )

select * from final
