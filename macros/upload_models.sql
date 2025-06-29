{% macro upload_models(graph, path=None, materialization=[]) -%}
    {% set models = dbt_observability.get_models_list(graph, path, materialization) %}
    {{ return(adapter.dispatch('get_models_dml_sql', 'dbt_observability')(models)) }}
{%- endmacro %}

{% macro default__get_models_dml_sql(models) -%}

    {% if models != [] %}
        {% set model_values %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

        select
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(5) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(6) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(7)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(12)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(13)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(15) }}
        from values

        {% endif %}

        {% for model in models -%}
            {%- set model_relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.name) -%}
            {% set table_exists=model_relation is not none %}

            {% if table_exists and model.config.materialized in ["table","incremental"] %}
                {% if target.type == 'snowflake' %}
                    {%- set rowcount_query %}
                    select row_count as model_rowcount
                    from information_schema.tables
                    where lower(table_name) = lower('{{ model.name }}')
                        and lower(table_schema) = lower('{{ model.schema }}')
                    {%- endset -%}
                {% else %}
                    {%- set rowcount_query %}
                    select count(*) as model_rowcount
                    from {{ model.schema }}.{{ model.name }}
                    {%- endset -%}
                {% endif %}
                {%- set results = run_query(rowcount_query) -%}
                {%- set raw_value = results.columns[0].values() | first -%}
                {%- set model_rowcount = 0 if raw_value is none else raw_value -%}
            {% else %}
                {%- set model_rowcount = 0 -%}
            {% endif %}

            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ model.database }}', {# database #}
                '{{ model.schema }}', {# schema #}
                '{{ model.name }}', {# name #}
                '{{ tojson(model.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ model.package_name }}', {# package_name #}
                '{{ model.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ model.checksum.checksum }}', {# checksum #}
                '{{ model.config.materialized }}', {# materialization #}
                '{{ tojson(model.tags) }}', {# tags #}
                '{{ tojson(model.config.meta) }}', {# meta #}
                '{{ null if model.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(model.description) }}', {# description #}
                {{ model_rowcount }}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_models_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_values %}
            {% for model in models -%}
            {%- set model_relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.name) -%}
            {% set table_exists=model_relation is not none %}

            {% if table_exists and model.config.materialized in ["table","incremental"] %}
                    {%- set rowcount_query %}
                    select count(*) as model_rowcount from {{ model.schema }}.{{ model.name }}
                    {%- endset -%}
                    {%- set results = run_query(rowcount_query) -%}
                    {%- set raw_value = results.columns[0].values() | first -%}
                    {%- set model_rowcount = 0 if raw_value is none else raw_value -%}
                {% else %}
                    {%- set model_rowcount = 0 -%}
                {% endif %}

                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ model.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ model.database }}', {# database #}
                    '{{ model.schema }}', {# schema #}
                    '{{ model.name }}', {# name #}
                    {{ tojson(model.depends_on.nodes) }}, {# depends_on_nodes #}
                    '{{ model.package_name }}', {# package_name #}
                    '{{ model.original_file_path | replace('\\', '\\\\') }}', {# path #}
                    '{{ model.checksum.checksum }}', {# checksum #}
                    '{{ model.config.materialized }}', {# materialization #}
                    {{ tojson(model.tags) }}, {# tags #}
                    parse_json('{{ tojson(model.config.meta) }}'), {# meta #}
                    '{{ null if model.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(model.description) }}', {# description #}
                    {{ model_rowcount }} {# total rowcount #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
