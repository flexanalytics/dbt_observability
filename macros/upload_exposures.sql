{% macro upload_exposures(graph) -%}
    {% set exposures = [] %}
    {% for node in graph.exposures.values() %}
        {% do exposures.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_exposures_dml_sql', 'dbt_observability')(exposures)) }}
{%- endmacro %}

{% macro default__get_exposures_dml_sql(exposures) -%}

    {% if exposures != [] %}
        {% set exposure_values %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

        select
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(5) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(6)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(7) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(12)) }}
        from values

        {% endif %}

        {% for exposure in exposures -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.unique_id) }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.name) }}', {# name #}
                '{{ exposure.type }}', {# type #}
                '{{ tojson(exposure.owner) }}', {# owner #}
                '{{ exposure.maturity }}', {# maturity #}
                '{{ exposure.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.description) }}', {# description #}
                '{{ exposure.url }}', {# url #}
                '{{ exposure.package_name }}', {# package_name #}
                '{{ tojson(exposure.depends_on.nodes) }}' {# depends_on_nodes #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ exposure_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_exposures_dml_sql(exposures) -%}
    {% if exposures != [] %}
        {% set exposure_values %}
            {% for exposure in exposures -%}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.unique_id) }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.name) }}', {# name #}
                    '{{ exposure.type }}', {# type #}
                    parse_json('{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(tojson(exposure.owner)) }}'), {# owner #}
                    '{{ exposure.maturity }}', {# maturity #}
                    '{{ exposure.original_file_path | replace('\\', '\\\\') }}', {# path #}
                    "{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(exposure.description) }}", {# description #}
                    '{{ exposure.url }}', {# url #}
                    '{{ exposure.package_name }}', {# package_name #}
                    {{ tojson(exposure.depends_on.nodes) }} {# depends_on_nodes #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ exposure_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
