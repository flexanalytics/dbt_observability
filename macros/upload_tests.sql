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
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(5)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(6) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(7) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(8)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(9)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(12) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(13) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(15) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(16) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(17) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(18) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(19) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(20) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(21) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(22) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(23) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(24) }}
        from values

        {% endif %}

        {% for test in tests -%}
            {% if test.description %}
                {% set test_description = test.description | replace("'", "\\'") %}
            {% else %}
                {% set test_description = "" %}
            {% endif %}
            {# test_metadata is a TestMetadata object (not a dict) -- use attribute
               access (Jinja resolves it for objects and dicts alike); kwargs is a dict #}
            {% set metadata = test.test_metadata if test.test_metadata is defined and test.test_metadata else none %}
            {% set kwargs = metadata.kwargs if metadata else {} %}
            {% set combination = kwargs.get('combination_of_columns') %}
            {% set test_attached_node = test.attached_node if test.attached_node is defined else none %}
            {% set test_package = metadata.namespace if metadata else none %}
            {% set test_type_name = metadata.name if metadata else none %}
            {# model is a raw get_where_subquery(ref()) expression -- keep just the model name #}
            {% set test_model = kwargs.get('model') %}
            {% set test_model_names = modules.re.findall("'([^']*)'", test_model | string) if test_model else [] %}
            {% set test_model = test_model_names[-1] if test_model_names else test_model %}
            {% set test_combination = combination | join(', ') if combination else none %}
            {# to / compare_model is a raw ref()/source() expression -- keep just the model name #}
            {% set test_to_model = kwargs.get('to') or kwargs.get('compare_model') %}
            {% set test_to_model_names = modules.re.findall("'([^']*)'", test_to_model | string) if test_to_model else [] %}
            {% set test_to_model = test_to_model_names[-1] if test_to_model_names else test_to_model %}
            {% set test_column_value = kwargs.get('value') %}
            {% set test_min_value = kwargs.get('min_value') %}
            {% set test_max_value = kwargs.get('max_value') %}
            {% set test_from_condition = kwargs.get('from_condition') %}
            {% set test_to_field = kwargs.get('field') %}
            {% set test_to_condition = kwargs.get('to_condition') %}
            {% set test_expression = kwargs.get('expression') %}
            {# Create a row for each test #}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ test.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ test.name }}', {# name #}
                '{{ tojson(test.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ test.package_name }}', {# package_name #}
                '{{ test.original_file_path | replace('\\', '\\\\') }}', {# test_path #}
                '{{ tojson(test.tags) }}', {# tags #}
                '{{ null if test.test_metadata is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(tojson(test.test_metadata)) }}', {# test.test_metadata #}
                '{{ test_description }}', {# description #}
                '{{ null if test.column_name is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test.column_name | string) }}', {# column_name #}
                '{{ null if test_attached_node is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_attached_node | string) }}', {# attached_node #}
                '{{ null if test_package is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_package | string) }}', {# test_package #}
                '{{ null if test_type_name is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_type_name | string) }}', {# test_name #}
                '{{ null if test_model is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_model | string) }}', {# test_model #}
                '{{ null if test_combination is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_combination | string) }}', {# test_combination_of_columns #}
                '{{ null if test_column_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_column_value | string) }}', {# test_column_value #}
                '{{ null if test_min_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_min_value | string) }}', {# test_column_min_value #}
                '{{ null if test_max_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_max_value | string) }}', {# test_column_max_value #}
                '{{ null if test_from_condition is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_from_condition | string) }}', {# test_relationship_from_model_condition #}
                '{{ null if test_to_model is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_model | string) }}', {# test_to_model #}
                '{{ null if test_to_field is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_field | string) }}', {# test_relationship_to_field #}
                '{{ null if test_to_condition is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_condition | string) }}', {# test_relationship_to_model_condition #}
                '{{ null if test_expression is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_expression | string) }}' {# test_column_expression #}
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
                {# test_metadata is a TestMetadata object (not a dict) -- use attribute
                   access (Jinja resolves it for objects and dicts alike); kwargs is a dict #}
                {% set metadata = test.test_metadata if test.test_metadata is defined and test.test_metadata else none %}
                {% set kwargs = metadata.kwargs if metadata else {} %}
                {% set combination = kwargs.get('combination_of_columns') %}
                {% set test_attached_node = test.attached_node if test.attached_node is defined else none %}
                {% set test_package = metadata.namespace if metadata else none %}
                {% set test_type_name = metadata.name if metadata else none %}
                {# model is a raw get_where_subquery(ref()) expression -- keep just the model name #}
                {% set test_model = kwargs.get('model') %}
                {% set test_model_names = modules.re.findall("'([^']*)'", test_model | string) if test_model else [] %}
                {% set test_model = test_model_names[-1] if test_model_names else test_model %}
                {% set test_combination = combination | join(', ') if combination else none %}
                {# to / compare_model is a raw ref()/source() expression -- keep just the model name #}
                {% set test_to_model = kwargs.get('to') or kwargs.get('compare_model') %}
                {% set test_to_model_names = modules.re.findall("'([^']*)'", test_to_model | string) if test_to_model else [] %}
                {% set test_to_model = test_to_model_names[-1] if test_to_model_names else test_to_model %}
                {% set test_column_value = kwargs.get('value') %}
                {% set test_min_value = kwargs.get('min_value') %}
                {% set test_max_value = kwargs.get('max_value') %}
                {% set test_from_condition = kwargs.get('from_condition') %}
                {% set test_to_field = kwargs.get('field') %}
                {% set test_to_condition = kwargs.get('to_condition') %}
                {% set test_expression = kwargs.get('expression') %}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ test.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ test.name }}', {# name #}
                    {{ tojson(test.depends_on.nodes) }}, {# depends_on_nodes #}
                    '{{ test.package_name }}', {# package_name #}
                    '{{ test.original_file_path | replace('\\', '\\\\') }}', {# test_path #}
                    {{ tojson(test.tags) }}, {# tags #}
                    parse_json('{{ tojson(test.test_metadata) }}'), {# test_metadata #}
                    '{{ test_description }}', {# description #}
                    '{{ null if test.column_name is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test.column_name | string) }}', {# column_name #}
                    '{{ null if test_attached_node is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_attached_node | string) }}', {# attached_node #}
                    '{{ null if test_package is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_package | string) }}', {# test_package #}
                    '{{ null if test_type_name is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_type_name | string) }}', {# test_name #}
                    '{{ null if test_model is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_model | string) }}', {# test_model #}
                    '{{ null if test_combination is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_combination | string) }}', {# test_combination_of_columns #}
                    '{{ null if test_column_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_column_value | string) }}', {# test_column_value #}
                    '{{ null if test_min_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_min_value | string) }}', {# test_column_min_value #}
                    '{{ null if test_max_value is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_max_value | string) }}', {# test_column_max_value #}
                    '{{ null if test_from_condition is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_from_condition | string) }}', {# test_relationship_from_model_condition #}
                    '{{ null if test_to_model is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_model | string) }}', {# test_to_model #}
                    '{{ null if test_to_field is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_field | string) }}', {# test_relationship_to_field #}
                    '{{ null if test_to_condition is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_to_condition | string) }}', {# test_relationship_to_model_condition #}
                    '{{ null if test_expression is none else adapter.dispatch('escape_singlequote', 'dbt_observability')(test_expression | string) }}' {# test_column_expression #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ test_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
