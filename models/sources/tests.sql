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
    cast(null as {{ type_string() }}) as description,
    cast(null as {{ type_string() }}) as column_name,
    cast(null as {{ type_string() }}) as attached_node,
    -- parsed from test_metadata at upload time (see upload_tests.sql)
    cast(null as {{ type_string() }}) as test_package,
    cast(null as {{ type_string() }}) as test_name,
    cast(null as {{ type_string() }}) as test_model,
    cast(null as {{ type_string() }}) as test_combination_of_columns,
    cast(null as {{ type_string() }}) as test_column_value,
    cast(null as {{ type_string() }}) as test_column_min_value,
    cast(null as {{ type_string() }}) as test_column_max_value,
    cast(null as {{ type_string() }}) as test_relationship_from_model_condition,
    cast(null as {{ type_string() }}) as test_to_model,
    cast(null as {{ type_string() }}) as test_relationship_to_field,
    cast(null as {{ type_string() }}) as test_relationship_to_model_condition,
    cast(null as {{ type_string() }}) as test_column_expression
from dummy_cte
where 1 = 0
