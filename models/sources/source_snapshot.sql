with
    dummy_cte as (
        select 1 as foo
    )

select
    cast(null as {{ type_string() }}) as table_name,
    cast(null as {{ type_string() }}) as column_name,
    cast(null as {{ type_string() }}) as data_type,
    cast(null as {{ type_string() }}) as table_schema
from dummy_cte
where 1 = 0
