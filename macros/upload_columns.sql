{% macro upload_columns(graph, path=None, materialization=None) -%}
    {% set models = dbt_observability.get_models_list(graph, path, materialization) %}
    {{ return(adapter.dispatch('get_columns_dml_sql', 'dbt_observability')(models)) }}
{%- endmacro %}

{% macro get_observability_config_from_node(node) %}
    {% set res = {} %}
    {% set node_config = node.get('config') %}
    {% if node_config %}
        {% set config = node.config.get('dbt_observability') %}
        {% if config and config is mapping %}
            {% do res.update(config) %}
        {% endif %}
        {% set config_meta = node.config.get('meta') %}
        {% if config_meta and config_meta is mapping %}
            {% set config = config_meta.get('dbt_observability') %}
            {% if config and config is mapping %}
                {% do res.update(config) %}
            {% endif %}
        {% endif %}
    {% endif %}
    {% set node_meta = node.get('meta') %}
    {% if node_meta and node_meta is mapping %}
        {% set config = node_meta.get('dbt_observability') %}
        {% if config and config is mapping %}
            {% do res.update(config) %}
        {% endif %}
    {% endif %}
    {{ return(res) }}
{% endmacro %}

{% macro get_observability_value(argument_name, node) %}
    {% set config = dbt_observability.get_observability_config_from_node(node) %}
    {{ print(config) }}
    {% if config and config is mapping %}
        {%- set model_config_value = config.get(argument_name) %}
        {%- if model_config_value %}
            {{ return(model_config_value) }}
        {%- endif %}
    {% endif %}
    {% set projectVar = var("dbt_observability:" ~ argument_name, "None") %}
    {% if projectVar %}
        {{ return(projectVar) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro is_metric(column, model) %}
    {% if not execute %}
    {% for metric in graph.metrics.values() | selectattr('expression'|lower, 'equalto', column.name.lower()) %}
        {% set mRef = metric.refs | selectattr('name'|lower, 'equalto', model.name.lower()) | first %}
        {% if mRef is defined %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}
    {% endif %}

    {{ return(false) }}

{% endmacro %}

{% macro default__get_columns_dml_sql(models) -%}

    {% if models != [] %}
        {% set model_values %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

        select
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(4) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(5)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(6)) }},
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

        {% set new_list = [] %}
        {% for model in models  %}
            {% if model.columns %}
            {% do new_list.append( model ) %}
            {% endif %}
        {% endfor %}

        {% for model in new_list -%}

            {% set lowerCols = {} %}
            {% for k, v in model.columns.items()  %}
                {% do lowerCols.update({k.lower(): v}) %}
            {% endfor %}

            {% set relation = dbt_observability.get_relation( model.name ) %}
            {%- set columns = adapter.get_columns_in_relation(relation) -%}

            {% set statsType = dbt_observability.get_observability_value('column_stats_type', model) %}

            {% set statsCols = [] %}

            {% if target.schema == model.schema and statsType is not none and statsType != 'NONE' %}

                {% set statsPick = dbt_observability.get_observability_value('column_stats_pick', model) %}
                {% set valsMax = dbt_observability.get_observability_value('column_values_max', model) %}
                {% if valsMax is none %}
                    {% set valsMax = 10 %}
                {% endif %}

                {% for column in columns %}

                    {% set isMetric = dbt_observability.is_metric(column, model) %}

                    {% if statsType == 'METRIC' %}

                        {% if isMetric %}
                        {% do statsCols.append( column ) %}
                        {% endif %}

                    {% elif statsType == 'DOC' %}

                        {% set col = lowerCols.get(column.name.lower()) %}
                        {% if col.name is defined or isMetric %}
                            {% do statsCols.append( column ) %}
                        {% endif %}

                    {% elif statsType == 'PICK' %}

                        {% set picked = statsPick | select('equalto', column.name.lower()) | first %}
                        {% if picked is not none %}
                            {% do statsCols.append( column ) %}
                        {% endif %}

                    {% endif %}

                {% endfor %}

                {% if(statsCols|length > 0) %}

                    {%- set col_query -%}
                    select
                        count(1) as "row_count"
                        {% for column in statsCols %}
                        , count(distinct {{ column.name }}) as "{{ column.name }}_distinct"
                        , count(1) - count({{ column.name }}) as "{{ column.name }}_null"
                        {% if column.is_number() %}
                        , min({{ column.name }}) as "{{ column.name }}_min"
                        , max({{ column.name }}) as "{{ column.name }}_max"
                        , avg({{ column.name }}) as "{{ column.name }}_avg"
                        , sum({{ column.name }}) as "{{ column.name }}_sum"
                        , {{ dbt_observability.stddev() }}({{ column.name }}) as "{{ column.name }}_stdev"
                        {% else %}
                        , null as "{{ column.name }}_min"
                        , null as "{{ column.name }}_max"
                        , null as "{{ column.name }}_avg"
                        , null as "{{ column.name }}_sum"
                        , null as "{{ column.name }}_stdev"
                        {% endif %}
                        {% endfor %}
                        {% for column in columns %}
                        {% set isMetric = dbt_observability.is_metric(column, model) %}
                        {% set col = lowerCols.get(column.name.lower()) %}
                        {% if statsType!='METRIC' and column.is_string() and col.name is defined and not isMetric and target.type != 'redshift' %}
                        , {{ dbt.listagg("distinct "~column.name, "', '", "order by "~column.name, valsMax) }} as "{{ column.name }}_values"
                        {% else %}
                        , null as "{{ column.name }}_values"
                        {% endif %}
                        {% endfor %}
                    from {{relation}}
                    {%- endset -%}
                    {%- set results = run_query(col_query) -%}

                {% else %}

                    {%- set results = none -%}

                {% endif %}

            {% else %}

                {%- set results = none -%}

            {% endif %}

            {% for column in columns %}
                {% set metric = graph.metrics.values() | selectattr('expression'|lower, 'equalto', column.name.lower()) | first %}
                {% set isMetric = dbt_observability.is_metric(column, model) %}
                {% set col = lowerCols.get(column.name.lower()) %}
                (
                    '{{ invocation_id }}' {# command_invocation_id #}
                    , '{{ model.unique_id }}' {# node_id #}
                    , '{{ column.name }}' {# column_name #}
                    , '{{ column.data_type }}' {# data_type #}
                    {% if target.type == 'bigquery' %}
                    , {{ '[]' if col.tags is not defined else tojson(col.tags) }} {# tags #}
                    , parse_json('{{ '{}' if col.meta is not defined else tojson(col.meta) }}') {# meta #}
                    {% else %}
                    , '{{ null if col.tags is not defined else tojson(col.tags) }}' {# tags #}
                    , '{{ null if col.meta is not defined else tojson(col.meta) }}' {# meta #}
                    {% endif %}
                    , '{{ null if col.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(col.description)}}' {# description #}
                    , '{{ "Y" if col.name is defined or isMetric else "N" }}' {# is_documented #}
                    {% set statsCol = statsCols | selectattr('name', 'equalto', column.name) | first  %}
                    , {{ (results and results.columns['row_count'].values()[0]) or 'null' }} {# row_count #}
                    , {{ (results and results.columns[column.name ~ '_distinct'] and results.columns[column.name ~ '_distinct'].values()[0]) or 'null' }} {# row_distinct #}
                    , {{ (results and results.columns[column.name ~ '_null'] and results.columns[column.name ~ '_null'].values()[0]) or 'null' }} {# row_null #}
                    , {{ (results and results.columns[column.name ~ '_min'] and results.columns[column.name ~ '_min'].values()[0]) or 'null' }} {# row_min #}
                    , {{ (results and results.columns[column.name ~ '_max'] and results.columns[column.name ~ '_max'].values()[0]) or 'null' }} {# row_max #}
                    , {{ (results and results.columns[column.name ~ '_avg'] and results.columns[column.name ~ '_avg'].values()[0]) or 'null' }} {# row_avg #}
                    , {{ (results and results.columns[column.name ~ '_sum'] and results.columns[column.name ~ '_sum'].values()[0]) or 'null' }} {# row_sum #}
                    , {{ (results and results.columns[column.name ~ '_stdev'] and results.columns[column.name ~ '_stdev'].values()[0]) or 'null' }} {# row_stdev #}
                    , '{{ "Y" if isMetric else "N" }}' {# is_metric #}
                    , '{{ (results and results.columns[column.name ~ '_values'] and results.columns[column.name ~ '_values'].values()[0]) or null }}' {# column_values #}
                )
                {%- if not loop.last %},{%- endif %}
            {% endfor %}
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}
