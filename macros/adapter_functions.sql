{% macro stddev() -%}
  {{ return(adapter.dispatch('stddev','dbt_observability')()) }}
{%- endmacro %}

{% macro default__stddev() -%}
    stddev
{%- endmacro %}

{% macro sqlserver__stddev() -%}
    stdev
{%- endmacro %}

{% macro log_observability() -%}
  {{ return(adapter.dispatch('log_observability','dbt_observability')()) }}
{%- endmacro %}

{% macro default__log_observability() -%}
{%- endmacro %}
