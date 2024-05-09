/* Bigquery won't let us `where` without `from` so we use this workaround */
with
    dummy_cte as (
        select 1 as foo
    )

select
    cast(null as {{ type_string() }}) as command_invocation_id,
    cast(null as {{ type_string() }}) as node_id,
    cast(null as {{ type_timestamp() }}) as run_started_at,
    cast(null as {{ type_string() }}) as was_full_refresh,
    cast(null as {{ type_string() }}) as thread_id,
    cast(null as {{ type_string() }}) as status,
    cast(null as {{ type_timestamp() }}) as compile_started_at,
    cast(null as {{ type_timestamp() }}) as query_completed_at,
    cast(null as {{ type_float() }}) as total_node_runtime,
    cast(null as {{ type_string() }}) as materialization,
    cast(null as {{ type_string() }}) as schema_name,
    cast(null as {{ type_string() }}) as name,
    cast(null as {{ type_string() }}) as resource_type
from dummy_cte
where 1 = 0
