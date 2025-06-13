{% macro upload_metrics(graph) -%}
    {% set metrics = [] %}
    {% for node in graph.metrics.values() %}
        {% do metrics.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_metrics_dml_sql', 'dbt_observability')(metrics)) }}
{%- endmacro %}

{% macro default__get_metrics_dml_sql(metrics) -%}

    {% if metrics != [] %}
        {% set metric_values %}

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
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(8)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(9)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(10)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(12)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(13)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(14) }}
        from values

        {% endif %}

        {% for metric in metrics -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ metric.unique_id }}', {# unique_id #}
                '{{ metric.name }}', {# name #}
                '{{ metric.label }}', {# label #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(metric.model) }}', {# model - better way to escape??? #}
                '{{ metric.expression }}', {# expression #}
                '{{ metric.calculation_method }}', {# calculation_method #}
                '{{ tojson(metric.filters) }}', {# filters #}
                '{{ tojson(metric.time_grains) }}', {# time_grains #}
                '{{ tojson(metric.dimensions) }}', {# dimensions #}
                '{{ metric.package_name }}', {# package_name #}
                '{{ tojson(metric.meta) }}', {# meta #}
                '{{ tojson(metric.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ null if metric.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(metric.description) }}' {# description #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ metric_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}
