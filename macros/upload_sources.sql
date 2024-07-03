{% macro upload_sources(graph) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_sources_dml_sql', 'dbt_observability')(sources)) }}
{%- endmacro %}

{% macro default__get_sources_dml_sql(sources) -%}

    {% if sources != [] %}
        {% set source_values %}

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
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(11)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(12) }}
        from values

        {% endif %}

        {% for source in sources -%}
        {% if var('dbt_observability:track_source_rowcounts', false) %}
            {% if target.type == 'snowflake' %}
                {%- set rowcount_query %}
                select row_count as source_rowcount
                from {{ source.database }}.information_schema.tables
                where lower(table_name) = lower('{{ source.identifier }}')
                    and lower(table_schema) = lower('{{ source.schema }}')
                {%- endset -%}
            {% else %}
                {%- set rowcount_query %}
                select count(*) as source_rowcount
                from {{ source.database }}.{{ source.schema }}.{{ source.identifier }}
                {%- endset -%}
            {% endif %}
            {%- set results = run_query(rowcount_query) -%}
            {%- set source_rowcount = results.columns[0].values()[0] -%}
        {% else %}
            {%- set source_rowcount = 0 -%}
        {% endif %}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ source.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ source.database }}', {# database #}
                '{{ source.schema }}', {# schema #}
                '{{ source.source_name }}', {# source_name #}
                '{{ source.loader }}', {# loader #}
                '{{ source.name }}', {# name #}
                '{{ source.identifier }}', {# identifier #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(source.loaded_at_field) }}', {# loaded_at_field #}
                '{{ tojson(adapter.dispatch('escape_singlequote', 'dbt_observability')(source.freshness)) }}', {# freshness #}
                {{ 0 if source_rowcount is not defined else source_rowcount }} {# source_rowcount #}

            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ source_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_sources_dml_sql(sources) -%}
    {% if sources != [] %}
        {% set source_values %}
            {% for source in sources -%}
            {% if var('dbt_observability:track_source_rowcounts', false) %}
                {%- set rowcount_query %}
                select row_count as source_rowcount
                from {{ source.database }}.information_schema.tables
                where lower(table_name) = lower('{{ source.identifier }}')
                    and lower(table_schema) = lower('{{ source.schema }}')
                {%- endset -%}
            {%- set results = run_query(rowcount_query) -%}
            {%- set source_rowcount = results.columns[0].values()[0] -%}
            {% else %}
                {%- set source_rowcount = 0 -%}
            {% endif %}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ source.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ source.database }}', {# database #}
                    '{{ source.schema }}', {# schema #}
                    '{{ source.source_name }}', {# source_name #}
                    '{{ source.loader }}', {# loader #}
                    '{{ source.name }}', {# name #}
                    '{{ source.identifier }}', {# identifier #}
                    '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(source.loaded_at_field) }}', {# loaded_at_field #}
                    parse_json('{{ tojson(source.freshness) }}'), {# freshness #}
                    {{ 0 if source_rowcount is not defined else source_rowcount }} {# source_rowcount #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ source_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
