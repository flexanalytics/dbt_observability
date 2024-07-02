{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    metrics as (
        select
            command_invocation_id,
            unique_id as node_id,  -- TODO: split to cols?
            name,
            label,
            model,
            expression,
            calculation_method,
            filters,
            time_grains,
            dimensions,
            package_name,
            meta,
            depends_on_nodes,
            description
        from {{ ref('stg_metric') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as metric_key,
    node_id,
    name,
    label,
    model,
    expression,
    calculation_method,
    filters,
    time_grains,
    dimensions,
    package_name,
    meta,
    depends_on_nodes,
    description
from metrics
