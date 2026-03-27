{% macro upload_models(graph, path=None, materialization=[], models=None) -%}
    {% if models is none %}
        {% set models = dbt_observability.get_models_list(graph, path, materialization) %}
    {% endif %}
    {{ return(adapter.dispatch('get_models_dml_sql', 'dbt_observability')(models)) }}
{%- endmacro %}

{% macro default__get_models_dml_sql(models) -%}

    {% if models != [] %}
        {% set rowcount_paths = var('dbt_observability:model_rowcount_paths', []) %}
        {% set track_rowcounts = var('dbt_observability:track_model_rowcounts', true) %}

        {# For Snowflake: single pre-loop to collect relation info and run one batch rowcount query #}
        {% set relation_cache = {} %}
        {% set rowcount_map = {} %}
        {% if track_rowcounts and target.type == 'snowflake' %}
            {% set eligible_names = [] %}
            {% set eligible_schemas = [] %}
            {% for model in models %}
                {% set model_relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.name) %}
                {% do relation_cache.update({model.unique_id: model_relation is not none}) %}
                {% if model_relation is not none and model.config.materialized in ["table","incremental"] %}
                    {% set orig_path = model.original_file_path | replace('\\', '/') %}
                    {% set ns = namespace(should_count=not rowcount_paths) %}
                    {% for rpath in rowcount_paths %}
                        {% if orig_path.startswith(rpath) %}
                            {% set ns.should_count = true %}
                        {% endif %}
                    {% endfor %}
                    {% if ns.should_count %}
                        {% do eligible_names.append(model.name | lower) %}
                        {% do eligible_schemas.append(model.schema | lower) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
            {% if eligible_names %}
                {%- set batch_rowcount_query %}
                select lower(table_name) as tname, lower(table_schema) as tschema, row_count as model_rowcount
                from information_schema.tables
                where lower(table_name) in ('{{ eligible_names | unique | join("', '") }}')
                    and lower(table_schema) in ('{{ eligible_schemas | unique | join("', '") }}')
                {%- endset %}
                {% set batch_results = run_query(batch_rowcount_query) %}
                {% for row in batch_results.rows %}
                    {% do rowcount_map.update({row[1] ~ '.' ~ row[0]: row[2]}) %}
                {% endfor %}
            {% endif %}
        {% endif %}

        {% set model_values %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

        select
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(4) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(5) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(6) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(7)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(8) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(9) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(10) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(12)) }},
            {{ adapter.dispatch('parse_json', 'dbt_observability')(adapter.dispatch('column_identifier', 'dbt_observability')(13)) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(14) }},
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(15) }}
        from values

        {% endif %}

        {% for model in models -%}
            {% if target.type == 'snowflake' %}
                {%- set table_exists = relation_cache.get(model.unique_id, false) -%}
            {% else %}
                {%- set model_relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.name) -%}
                {%- set table_exists = model_relation is not none -%}
            {% endif %}

            {% if track_rowcounts and table_exists and model.config.materialized in ["table","incremental"] %}
                {% if target.type == 'snowflake' %}
                    {%- set rc_key = (model.schema | lower) ~ '.' ~ (model.name | lower) -%}
                    {%- set model_rowcount = rowcount_map.get(rc_key, 0) -%}
                {% else %}
                    {% set orig_path = model.original_file_path | replace('\\', '/') %}
                    {% set ns = namespace(should_count=not rowcount_paths) %}
                    {% for rpath in rowcount_paths %}
                        {% if orig_path.startswith(rpath) %}
                            {% set ns.should_count = true %}
                        {% endif %}
                    {% endfor %}
                    {% if ns.should_count %}
                        {%- set rowcount_query %}
                        select count(*) as model_rowcount
                        from {{ model.schema }}.{{ model.name }}
                        {%- endset -%}
                        {%- set results = run_query(rowcount_query) -%}
                        {%- set raw_value = results.columns[0].values() | first -%}
                        {%- set model_rowcount = 0 if raw_value is none else raw_value -%}
                    {% else %}
                        {%- set model_rowcount = 0 -%}
                    {% endif %}
                {% endif %}
            {% else %}
                {%- set model_rowcount = 0 -%}
            {% endif %}

            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ model.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ model.database }}', {# database #}
                '{{ model.schema }}', {# schema #}
                '{{ model.name }}', {# name #}
                '{{ tojson(model.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ model.package_name }}', {# package_name #}
                '{{ model.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ model.checksum.checksum }}', {# checksum #}
                '{{ model.config.materialized }}', {# materialization #}
                '{{ tojson(model.tags) }}', {# tags #}
                '{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(tojson(model.config.meta)) }}', {# meta #}
                '{{ null if model.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(model.description) }}', {# description #}
                {{ model_rowcount }}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_models_dml_sql(models) -%}
    {% if models != [] %}
        {% set model_values %}
            {% set rowcount_paths = var('dbt_observability:model_rowcount_paths', []) %}
            {% set track_rowcounts = var('dbt_observability:track_model_rowcounts', true) %}
            {% for model in models -%}
            {%- set model_relation = adapter.get_relation(database=model.database, schema=model.schema, identifier=model.name) -%}
            {% set table_exists=model_relation is not none %}

            {% if track_rowcounts and table_exists and model.config.materialized in ["table","incremental"] %}
                {% set orig_path = model.original_file_path | replace('\\', '/') %}
                {% set ns = namespace(should_count=not rowcount_paths) %}
                {% for rpath in rowcount_paths %}
                    {% if orig_path.startswith(rpath) %}
                        {% set ns.should_count = true %}
                    {% endif %}
                {% endfor %}
                {% if ns.should_count %}
                    {%- set rowcount_query %}
                    select count(*) as model_rowcount from {{ model.schema }}.{{ model.name }}
                    {%- endset -%}
                    {%- set results = run_query(rowcount_query) -%}
                    {%- set raw_value = results.columns[0].values() | first -%}
                    {%- set model_rowcount = 0 if raw_value is none else raw_value -%}
                {% else %}
                    {%- set model_rowcount = 0 -%}
                {% endif %}
                {% else %}
                    {%- set model_rowcount = 0 -%}
                {% endif %}

                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ model.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ model.database }}', {# database #}
                    '{{ model.schema }}', {# schema #}
                    '{{ model.name }}', {# name #}
                    {{ tojson(model.depends_on.nodes) }}, {# depends_on_nodes #}
                    '{{ model.package_name }}', {# package_name #}
                    '{{ model.original_file_path | replace('\\', '\\\\') }}', {# path #}
                    '{{ model.checksum.checksum }}', {# checksum #}
                    '{{ model.config.materialized }}', {# materialization #}
                    {{ tojson(model.tags) }}, {# tags #}
                    parse_json('{{ adapter.dispatch('escape_singlequote', 'dbt_observability')(tojson(model.config.meta)) }}'), {# meta #}
                    '{{ null if model.description is not defined else adapter.dispatch('escape_singlequote', 'dbt_observability')(model.description) }}', {# description #}
                    {{ model_rowcount }} {# total rowcount #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
