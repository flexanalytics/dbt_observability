{% macro upload_sources(graph) -%}
    {% set sources = [] %}
    {% for node in graph.sources.values() %}
        {% do sources.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_sources_dml_sql', 'dbt_observability')(sources)) }}
{%- endmacro %}

{% macro default__get_sources_dml_sql(sources) -%}

    {% if sources != [] %}
        {% set track_rowcounts = var('dbt_observability:track_source_rowcounts', false) %}

        {# For Snowflake: pre-loop to collect relation info and run one batch rowcount query per database #}
        {% set relation_cache = {} %}
        {% set rowcount_map = {} %}
        {% if track_rowcounts and target.type == 'snowflake' %}
            {% set eligible_by_db = {} %}
            {% for source in sources %}
                {% set source_relation = adapter.get_relation(database=source.database, schema=source.schema, identifier=source.identifier) %}
                {% do relation_cache.update({source.unique_id: source_relation is not none}) %}
                {% if source_relation is not none %}
                    {% set db_key = source.database | lower %}
                    {% if db_key not in eligible_by_db %}
                        {% do eligible_by_db.update({db_key: {'db': source.database, 'names': [], 'schemas': []}}) %}
                    {% endif %}
                    {% do eligible_by_db[db_key]['names'].append(source.identifier | lower) %}
                    {% do eligible_by_db[db_key]['schemas'].append(source.schema | lower) %}
                {% endif %}
            {% endfor %}
            {% for db_key, db_info in eligible_by_db.items() %}
                {%- set batch_rowcount_query %}
                select lower(table_name) as tname, lower(table_schema) as tschema, row_count as source_rowcount
                from {{ db_info.db }}.information_schema.tables
                where lower(table_name) in ('{{ db_info.names | unique | join("', '") }}')
                    and lower(table_schema) in ('{{ db_info.schemas | unique | join("', '") }}')
                {%- endset %}
                {% set batch_results = run_query(batch_rowcount_query) %}
                {% for row in batch_results.rows %}
                    {% do rowcount_map.update({db_key ~ '.' ~ row[1] ~ '.' ~ row[0]: row[2]}) %}
                {% endfor %}
            {% endfor %}
        {% endif %}

        {% set source_values %}

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
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(11)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(12) }}
        from values

        {% endif %}

        {% for source in sources -%}
            {% if track_rowcounts %}
                {% if target.type == 'snowflake' %}
                    {%- set table_exists = relation_cache.get(source.unique_id, false) -%}
                    {% if table_exists %}
                        {%- set rc_key = (source.database | lower) ~ '.' ~ (source.schema | lower) ~ '.' ~ (source.identifier | lower) -%}
                        {%- set source_rowcount = rowcount_map.get(rc_key, 0) -%}
                    {% else %}
                        {%- set source_rowcount = 0 -%}
                        {%- do log("Warning, source does not exist: " ~ source.identifier, info=false) -%}
                    {% endif %}
                {% else %}
                    {%- set source_relation = adapter.get_relation(database=source.database, schema=source.schema, identifier=source.identifier) -%}
                    {% if source_relation is not none %}
                        {%- set rowcount_query %}
                        select count(*) as source_rowcount
                        from {{ source.database }}.{{ source.schema }}.{{ source.identifier }}
                        {%- endset -%}
                        {%- set results = run_query(rowcount_query) -%}
                        {%- set raw_value = results.columns[0].values() | first -%}
                        {%- set source_rowcount = 0 if raw_value is none else raw_value -%}
                    {% else %}
                        {%- set source_rowcount = 0 -%}
                        {%- do log("Warning, source does not exist: " ~ source.identifier, info=false) -%}
                    {% endif %}
                {% endif %}
            {% else %}
                {%- set source_rowcount = 0 -%}
            {% endif %}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ source.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ source.database }}', {# database #}
                '{{ source.schema }}', {# schema #}
                '{{ source.source_name }}', {# source_name #}
                '{{ source.loader }}', {# loader #}
                '{{ source.name }}', {# name #}
                '{{ source.identifier }}', {# identifier #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(source.loaded_at_field) }}', {# loaded_at_field #}
                '{{ tojson(adapter.dispatch('escape_singlequote', 'dbt_observability')(source.freshness)) }}', {# freshness #}
                {{ 0 if source_rowcount is none else source_rowcount }} {# source_rowcount #}

            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ source_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_sources_dml_sql(sources) -%}
    {% if sources != [] %}
        {% set source_values %}
            {% for source in sources -%}
            {% if var('dbt_observability:track_source_rowcounts', false) %}
                {%- set rowcount_query %}
                select row_count as source_rowcount
                from {{ source.database }}.information_schema.tables
                where lower(table_name) = lower('{{ source.identifier }}')
                    and lower(table_schema) = lower('{{ source.schema }}')
                {%- endset -%}
            {%- set results = run_query(rowcount_query) -%}
            {%- set raw_value = results.columns[0].values() | first -%}
            {%- set source_rowcount = 0 if raw_value is none else raw_value -%}
            {% else %}
                {%- set source_rowcount = 0 -%}
            {% endif %}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ source.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ source.database }}', {# database #}
                    '{{ source.schema }}', {# schema #}
                    '{{ source.source_name }}', {# source_name #}
                    '{{ source.loader }}', {# loader #}
                    '{{ source.name }}', {# name #}
                    '{{ source.identifier }}', {# identifier #}
                    '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(source.loaded_at_field) }}', {# loaded_at_field #}
                    parse_json('{{ tojson(source.freshness) }}'), {# freshness #}
                    {{ 0 if source_rowcount is none else source_rowcount }} {# source_rowcount #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ source_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
