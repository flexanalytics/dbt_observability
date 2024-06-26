{% macro insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}

    {% if content != "" %}
        {{ return(adapter.dispatch('insert_into_source_schema_table', 'dbt_observability')(database_name, schema_name, table_name, content)) }}
    {% endif %}
{%- endmacro %}

{% macro default__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}
    {% set insert_into_table_query %}
    insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
    VALUES
    {{ content }}
    {% endset %}

    {% do run_query(insert_into_table_query) %}

{%- endmacro %}

{% macro spark__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}
    {% set insert_into_table_query %}
    insert into {{ schema_name }}.{{ table_name }}
    {{ content }}
    {% endset %}

    {% do run_query(insert_into_table_query) %}
{%- endmacro %}

{% macro snowflake__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}
    {% set insert_into_table_query %}
    insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
    {{ content }}
    {% endset %}

    {% do run_query(insert_into_table_query) %}
{%- endmacro %}

{% macro TBD__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}

    {%- call statement('do_insert', fetch_result=False, auto_begin=True) -%}
    insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
    VALUES
    {{ content }}
    {%- endcall -%}
    {{ adapter.commit() }}

{%- endmacro %}

{% macro postgres__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}

    {% set insert_into_table_query %}
    begin;
    insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
    VALUES
    {{ content }};
    commit;
    {% endset %}
    {% do run_query(insert_into_table_query) %}

{%- endmacro %}

{% macro redshift__insert_into_source_schema_table(database_name, schema_name, table_name, content) -%}

    {% set insert_into_table_query %}
    begin;
    insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
    VALUES
    {{ content }};
    commit;
    {% endset %}

    {% do run_query(insert_into_table_query) %}

{%- endmacro %}
