{#- BOOLEAN -#}

{% macro type_boolean() %}
    {{ return(adapter.dispatch('type_boolean', 'dbt_observability')()) }}
{% endmacro %}

{% macro default__type_boolean() %}
   {{ return(api.Column.translate_type("boolean")) }}
{% endmacro %}

{#- JSON -#}

{% macro type_json() %}
    {{ return(adapter.dispatch('type_json', 'dbt_observability')()) }}
{% endmacro %}

{% macro default__type_json() %}
   {{ return(api.Column.translate_type("string")) }}
{% endmacro %}

{% macro snowflake__type_json() %}
   OBJECT
{% endmacro %}

{% macro bigquery__type_json() %}
   JSON
{% endmacro %}

{% macro redshift__type_json() %}
   varchar(max)
{% endmacro %}

{% macro duckdb__type_json() %}
   JSON
{% endmacro %}

{#- ARRAY -#}

{% macro type_array() %}
    {{ return(adapter.dispatch('type_array', 'dbt_observability')()) }}
{% endmacro %}

{% macro default__type_array() %}
   {{ return(api.Column.translate_type("string")) }}
{% endmacro %}

{% macro snowflake__type_array() %}
   ARRAY
{% endmacro %}

{% macro bigquery__type_array() %}
   ARRAY<string>
{% endmacro %}

{% macro redshift__type_array() %}
   varchar(max)
{% endmacro %}

{% macro duckdb__type_array() %}
   list
{% endmacro %}

{#- STRING -#}

{% macro type_string() %}
    {{ return(adapter.dispatch('type_string', 'dbt_observability')()) }}
{% endmacro %}

{% macro default__type_string() %}
   varchar(max)
{% endmacro %}

{% macro postgres__type_string() %}
   text
{% endmacro %}

{% macro bigquery__type_string() %}
   string
{% endmacro %}

{% macro snowflake__type_string() %}
   varchar(16777216)
{% endmacro %}

{% macro duckdb__type_string() %}
   varchar
{% endmacro %}

{#- TIMESTAMP -#}

{% macro sqlserver__type_timestamp() %}
   datetime
{% endmacro %}

