{% macro upload_source_snapshot(graph) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_source_columns_info', 'dbt_observability')(sources)) }}
{%- endmacro %}

{% macro default__get_source_columns_info(sources) -%}
    {% set source_schema = dbt_observability.get_relation('source_snapshot') %}
    {% set source_info_query %}
    select
        c.column_name,
        c.table_name,
        c.data_type,
        c.table_schema
    from information_schema.columns c
    left join {{ source_schema.database }}.{{ source_schema.schema }}.{{ source_schema.identifier }} s
        on c.table_name = s.table_name
    where c.table_schema in (
        {% for source in sources -%}
            '{{ source.schema }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    and c.table_name in (
        {% for source in sources -%}
            '{{ source.identifier }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    and s.table_name is null
    order by c.table_schema, c.table_name, c.column_name
    {% endset %}
    {% set result = run_query(source_info_query) %}
    {% set results_list = result.columns[0].values() %}
    {% if results_list | length > 0 %}
    {% set column_values %}
    {% if execute %}
        {% set columns, tables, types, schemas = result.columns[0].values(), result.columns[1].values(), result.columns[2].values(), result.columns[3].values()%}
        {% for column, table, type, schema in zip(columns, tables, types, schemas) %}
            (
                '{{ table }}',
                '{{ column }}',
                '{{ type }}',
                '{{ schema }}'
            )
            {%- if not loop.last %},{%- endif %}
        {% endfor %}
    {% endif %}
    {% endset %}
    {{ return (column_values) }}
    {% do log(column_values) %}
    {% endif %}
    {{ return ("") }}

{% endmacro %}
