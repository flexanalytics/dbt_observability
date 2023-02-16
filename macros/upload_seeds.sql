{% macro upload_seeds(graph) -%}
    {% set seeds = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "seed") | selectattr("package_name", "equalto", project_name) %}
        {% do seeds.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_seeds_dml_sql', 'dbt_observability')(seeds)) }}
{%- endmacro %}

{% macro default__get_seeds_dml_sql(seeds) -%}

    {% if seeds != [] %}
        {% set seed_values %}

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
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }}
        from values

        {% endif %}

        {% for seed in seeds -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ seed.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ seed.database }}', {# database #}
                '{{ seed.schema }}', {# schema #}
                '{{ seed.name }}', {# name #}
                '{{ seed.package_name }}', {# package_name #}
                '{{ seed.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ seed.checksum.checksum }}' {# checksum #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ seed_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_seeds_dml_sql(seeds) -%}
    {% if seeds != [] %}
        {% set seed_values %}
            {% for seed in seeds -%}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ seed.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ seed.database }}', {# database #}
                    '{{ seed.schema }}', {# schema #}
                    '{{ seed.name }}', {# name #}
                    '{{ seed.package_name }}', {# package_name #}
                    '{{ seed.original_file_path | replace('\\', '\\\\') }}', {# path #}
                    '{{ seed.checksum.checksum }}' {# checksum #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ seed_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
