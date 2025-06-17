{% macro upload_tests(graph) -%}
    {% set packages = var('dbt_observability:projects', [project_name]) %}
    {% set tests = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "test") | selectattr("package_name", "in", packages) %}
        {% do tests.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_tests_dml_sql', 'dbt_observability')(tests)) }}
{%- endmacro %}

{% macro default__get_tests_dml_sql(tests) -%}

    {% if tests != [] %}
        {% set test_values %}

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
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(9)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(10)) }}
        from values

        {% endif %}

        {% for test in tests -%}
            {% if test.description %}
                {% set test_description = test.description | replace("'", "\\'") %}
            {% else %}
                {% set test_description = "" %}
            {% endif %}
            {# Create a row for each test #}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ test.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ test.name }}', {# name #}
                '{{ test_description }}', {# description #}
                '{{ tojson(test.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ test.package_name }}', {# package_name #}
                '{{ test.original_file_path | replace('\\', '\\\\') }}', {# test_path #}
                '{{ tojson(test.tags) }}', {# tags #}
                '{{ null if test.test_metadata is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(tojson(test.test_metadata)) }}' {# test.test_metadata #}                
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ test_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_tests_dml_sql(tests) -%}
    {% if tests != [] %}
        {% set test_values %}
            {% for test in tests -%}
                {% if test.description %}
                    {% set test_description = test.description | replace("'", "\\'") %}
                {% else %}
                    {% set test_description = "" %}
                {% endif %}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ test.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ test.name }}', {# name #}
                    '{{ test_description }}', {# description #}
                    {{ tojson(test.depends_on.nodes) }}, {# depends_on_nodes #}
                    '{{ test.package_name }}', {# package_name #}
                    '{{ test.original_file_path | replace('\\', '\\\\') }}', {# test_path #}
                    {{ tojson(test.tags) }}, {# tags #}
                    parse_json('{{ tojson(test.test_metadata) }}') {# test_metadata #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ test_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
