{% macro get_columns_content(graph, resource_type) %}
    {% set columns = [] %}

    {% if resource_type == 'model' %}
        {% for model in graph.nodes.values() %}
            {% if model.resource_type == 'model' and model.columns %}
                {% for column in model.columns.values() %}
                    {% do columns.append({
                        'name': column.name,
                        'data_type': column.data_type,
                        'tags': column.tags,
                        'meta': column.meta,
                        'description': column.description,
                        'model': model
                    }) %}
                {% endfor %}
            {% endif %}
        {% endfor %}
        {{ return(columns) }}
    {% elif resource_type == 'source' %}
        {% set source_columns = [] %}
        {% set database_dict = {} %}

        {% for source in graph.sources.values() %}
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

        {% set source_info_query %}
        {% for database, schemas in database_dict.items() %}
            {% for schema, info in schemas.items() %}

        select
            '{{ info.package_name }}' as package_name,
            '{{ info.source_name }}' as source_name,
            lower(column_name) as column_name,
            lower(table_name) as table_name,
            lower(data_type) as data_type
        from {{ database }}.information_schema.columns
        where lower(table_schema) = '{{ schema }}'
        and lower(table_name) in (
                {%- for table in info.tables -%}
                    '{{ table }}'
                    {%- if not loop.last %}, {% endif %}
                {%- endfor %}
        )
                {%- if not loop.last %} union all {% endif %}
            {% endfor %}
        {% endfor %}
    {%- endset %}

    {% set result = run_query(source_info_query) %}
    {% endif %}
    {{ return(result | list) }}
{% endmacro %}