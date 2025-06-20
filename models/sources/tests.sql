/* Bigquery won't let us `where` without `from` so we use this workaround */
{{
    config(
        enabled=var('dbt_observability:tracking_enabled', true)
    )
}}
with
    dummy_cte as (
        select 1 as foo
    )

select
    cast(null as {{ type_string() }}) as command_invocation_id,
    cast(null as {{ type_string() }}) as node_id,
    cast(null as {{ type_timestamp() }}) as run_started_at,
    cast(null as {{ type_string() }}) as name,
    cast(null as {{ type_array() }}) as depends_on_nodes,
    cast(null as {{ type_string() }}) as package_name,
    cast(null as {{ type_string() }}) as test_path,
    cast(null as {{ type_array() }}) as tags,
    cast(null as {{ type_json() }}) as test_metadata,
    cast(null as {{ type_string() }}) as description
from dummy_cte
where 1 = 0
