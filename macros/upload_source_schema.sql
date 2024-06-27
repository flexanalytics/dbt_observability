{% macro upload_source_schema(graph) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_source_columns_info', 'dbt_observability')(sources)) }}
{%- endmacro %}

{% macro default__get_source_columns_info(sources) -%}
    {% set source_info_query %}
    select
        column_name,
        table_name,
        data_type
    from information_schema.columns
    where table_schema in (
        {% for source in sources -%}
            '{{ source.schema }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    and table_name in (
        {% for source in sources -%}
            '{{ source.identifier }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    order by table_name, ordinal_position
    {% endset %}
    {% set result = run_query(source_info_query) %}
    {% set column_values %}
    {% if execute %}
        {% set columns, tables, types = result.columns[0].values(), result.columns[1].values(), result.columns[2].values() %}
        {% for column, table, type in zip(columns, tables, types) %}
            (
                '{{ invocation_id }}',
                '{{ "source." ~ target.profile_name ~ "." ~ table }}',
                '{{ column }}',
                '{{ type }}',
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
            {%- if not loop.last %},{%- endif %}
        {% endfor %}
    {% endif %}
    {% endset %}
    {{ return (column_values) }}


{% endmacro %}
