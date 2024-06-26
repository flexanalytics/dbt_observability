{% macro get_source_state() -%}
    {% set path = var('dbt_observability:path',None) %}
    {% set materialization = var('dbt_observability:materialization',None) %}
    {% if execute and var('dbt_observability:enabled', false) and flags.WHICH not in ['generate','serve','test'] %}
        {% do log("Uploading source schemas", true) %}
        {% set source_schema = dbt_observability.get_relation('source_snapshot') %}
        {% set content_source_schema = dbt_observability.upload_source_snapshot(graph) %}
        {{ dbt_observability.insert_into_source_schema_table(
            database_name=source_schema.database,
            schema_name=source_schema.schema,
            table_name=source_schema.identifier,
            content=content_source_schema
            )
        }}
    {% endif %}
{%- endmacro %}
