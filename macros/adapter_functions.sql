{% macro stddev() -%}
  {{ return(adapter.dispatch('stddev')()) }}
{%- endmacro %}

{% macro default__stddev() -%}
    stddev
{%- endmacro %}

{% macro sqlserver__stddev() -%}
    stdev
{%- endmacro %}