{% macro upload_columns(graph) -%}
    {% set models = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("package_name", "equalto", project_name) %}
        {% do models.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_columns_dml_sql', 'dbt_observability')(models)) }}
{%- endmacro %}

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
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(16) }}
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

            {% if target.schema == model.schema %}

            {%- set col_query -%}
            select
                count(1) as row_count
                {% for column in columns %}
                , count(distinct {{ column.name }}) as {{ column.name }}_distinct
                , count(1) - count({{ column.name }}) as {{ column.name }}_null
                {% if column.is_number() %}
                , min({{ column.name }}) as {{ column.name }}_min
                , max({{ column.name }}) as {{ column.name }}_max
                , avg({{ column.name }}) as {{ column.name }}_avg
                , sum({{ column.name }}) as {{ column.name }}_sum
                {% if target.type == 'sqlserver' %}
                , stdev({{ column.name }}) as {{ column.name }}_stdev
                {% else %}
                , stddev({{ column.name }}) as {{ column.name }}_stdev
                {% endif %}
                {% else %}
                , null as {{ column.name }}_min
                , null as {{ column.name }}_max
                , null as {{ column.name }}_avg
                , null as {{ column.name }}_sum
                , null as {{ column.name }}_stdev
                {% endif %}
                {% endfor %}
            from {{relation}}
            {%- endset -%}
            {%- set results = run_query(col_query) -%}

            {% else %}
            {%- set results = none -%}
            {% endif %}

            {% for column in columns %}
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
                    , '{{ null if col.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(col.description) }}' {# description #}
                    , '{{ "N" if col.name is not defined else "Y" }}' {# is_documented #}
                    {% if results is none %}
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    {% elif target.type == 'snowflake' %}
                    , {{ results.columns['ROW_COUNT'].values()[0] or 0 }} {# row_count #}
                    , {{ results.columns[column.name ~ '_DISTINCT'].values()[0] or 0 }} {# row_distinct #}
                    , {{ results.columns[column.name ~ '_NULL'].values()[0] or 0 }} {# row_null #}
                    , {{ results.columns[column.name ~ '_MIN'].values()[0] or 0 }} {# row_min #}
                    , {{ results.columns[column.name ~ '_MAX'].values()[0] or 0 }} {# row_max #}
                    , {{ results.columns[column.name ~ '_AVG'].values()[0] or 0 }} {# row_avg #}
                    , {{ results.columns[column.name ~ '_SUM'].values()[0] or 0 }} {# row_sum #}
                    , {{ results.columns[column.name ~ '_STDEV'].values()[0] or 0 }} {# row_stdev #}
                    {% else %}
                    , {{ results.columns['row_count'].values()[0] or 0 }} {# row_count #}
                    , {{ results.columns[column.name ~ '_distinct'].values()[0] or 0 }} {# row_distinct #}
                    , {{ results.columns[column.name ~ '_null'].values()[0] or 0 }} {# row_null #}
                    , {{ results.columns[column.name ~ '_min'].values()[0] or 0 }} {# row_min #}
                    , {{ results.columns[column.name ~ '_max'].values()[0] or 0 }} {# row_max #}
                    , {{ results.columns[column.name ~ '_avg'].values()[0] or 0 }} {# row_avg #}
                    , {{ results.columns[column.name ~ '_sum'].values()[0] or 0 }} {# row_sum #}
                    , {{ results.columns[column.name ~ '_stdev'].values()[0] or 0 }} {# row_stdev #}
                    {% endif %}
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
