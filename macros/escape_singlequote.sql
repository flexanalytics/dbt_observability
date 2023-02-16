{% macro escape_singlequote(value) -%}
  {{ return(adapter.dispatch('escape_singlequote')(value)) }}
{%- endmacro %}

{% macro default__escape_singlequote(value) -%}
    {{ value | replace("'","''") }}
{%- endmacro %}

{% macro bigquery__escape_singlequote(value) -%}
    {{ value | replace("'","\\'") }}
{%- endmacro %}
