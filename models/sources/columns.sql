/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as {{ type_string() }}) as command_invocation_id
    , cast(null as {{ type_string() }}) as node_id
    , cast(null as {{ type_string() }}) as column_name
    , cast(null as {{ type_string() }}) as data_type
    , cast(null as {{ type_array() }}) as tags
    , cast(null as {{ type_json() }}) as meta
    , cast(null as {{ type_string() }}) as description
    , cast(null as {{ type_string() }}) as is_documented
    , cast(null as {{ type_int() }}) as row_count
    , cast(null as {{ type_int() }}) as row_distinct
    , cast(null as {{ type_int() }}) as row_null
    , cast(null as {{ type_float() }}) as row_min
    , cast(null as {{ type_float() }}) as row_max
    , cast(null as {{ type_float() }}) as row_avg
    , cast(null as {{ type_float() }}) as row_sum
    , cast(null as {{ type_float() }}) as row_stdev
    , cast(null as {{ type_string() }}) as is_metric
    , cast(null as {{ type_string() }}) as column_values
from dummy_cte
where 1 = 0
