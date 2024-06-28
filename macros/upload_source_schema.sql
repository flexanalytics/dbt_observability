{% macro upload_source_schema(graph) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_source_columns_info', 'dbt_observability')(sources)) }}
{%- endmacro %}

{% macro default__get_source_columns_info(sources) -%}
    {% set source_schemas = [] %}
    {% for source in sources %}
        {% do source_schemas.append(source.schema) %}
    {% endfor %}
    {% set source_schemas = source_schemas | unique %}
    {% set source_databases = [] %}
    {% for source in sources %}
        {% do source_databases.append(source.database) %}
    {% endfor %}
    {% set source_databases = source_databases | unique %}
    {% set source_info_query %}
    {% for database in source_databases %}
    select
        column_name,
        table_name,
        data_type
    from {{ database }}.information_schema.columns
    where lower(table_schema) in (
        {% for schema in source_schemas -%}
            '{{ schema }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    and lower(table_name) in (
        {% for source in sources -%}
            '{{ source.identifier }}'
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    order by table_name, ordinal_position
    {%- if not loop.last %} union all {% endif %}
    {% endfor %}
    {%- endset %}
    {% set result = run_query(source_info_query) %}
    {% set column_values %}
    {% if execute %}
    {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}
        select
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(5) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(6) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(7) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(12) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(13) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(15) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(16) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(17) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(18) }}
        from values
        {% endif %}
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
