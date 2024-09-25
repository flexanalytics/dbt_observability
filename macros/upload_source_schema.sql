{% macro upload_source_schema(graph, columns) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_source_columns_info', 'dbt_observability')(columns, sources)) }}
{%- endmacro %}

{% macro default__get_source_columns_info(columns, sources) -%}
    {% set database_dict = {} %}

    {% for source in sources %}
        {% if database_dict[source.database] is not defined %}
            {% set _ = database_dict.update({
                source.database: {}
            }) %}
        {% endif %}

        {% if database_dict[source.database][source.schema] is not defined %}
            {% set _ = database_dict[source.database].update({
                source.schema: {
                    'tables': [],
                    'source_name': source.source_name,
                    'package_name': source.package_name
                }
            }) %}
        {% endif %}

        {% set _ = database_dict[source.database][source.schema]['tables'].append(source.identifier) %}
    {% endfor %}

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

            {% for column in columns %}

            (
                '{{ invocation_id }}',
                'source.{{ column.source_name }}.{{ column.package_name }}.{{ column.table_name }}',
                '{{ column.column_name }}',
                '{{ column.data_type }}',
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
