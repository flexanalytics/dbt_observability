/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as {{ type_string() }}) as command_invocation_id,
    cast(null as {{ type_string() }}) as unique_id,
    cast(null as {{ type_string() }}) as name,
    cast(null as {{ type_string() }}) as label,
    cast(null as {{ type_string() }}) as model,
    cast(null as {{ type_string() }}) as expression,
    cast(null as {{ type_string() }}) as calculation_method,
    cast(null as {{ type_array() }}) as filters,
    cast(null as {{ type_array() }}) as time_grains,
    cast(null as {{ type_array() }}) as dimensions,
    cast(null as {{ type_string() }}) as package_name,
    cast(null as {{ type_json() }}) as meta,
    cast(null as {{ type_array() }}) as depends_on_nodes,
    cast(null as {{ type_string() }}) as description
from dummy_cte
where 1 = 0
