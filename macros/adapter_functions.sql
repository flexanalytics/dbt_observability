{% macro stddev() -%}
  {{ return(adapter.dispatch('stddev','dbt_observability')()) }}
{%- endmacro %}

{% macro default__stddev() -%}
    stddev
{%- endmacro %}

{% macro sqlserver__stddev() -%}
    stdev
{%- endmacro %}