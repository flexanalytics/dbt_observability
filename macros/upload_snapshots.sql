{% macro upload_snapshots(graph) -%}
    {% set snapshots = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "snapshot") | selectattr("package_name", "equalto", project_name) %}
        {% do snapshots.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_snapshots_dml_sql', 'dbt_observability')(snapshots)) }}

{%- endmacro %}

{% macro default__get_snapshots_dml_sql(snapshots) -%}

    {% if snapshots != [] %}
        {% set snapshot_values %}

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
            {{ adapter.dispatch('column_identifier', 'dbt_observability')(11) }}
        from values

        {% endif %}

        {% for snapshot in snapshots -%}
            (
                '{{ invocation_id }}', {# command_invocation_id #}
                '{{ snapshot.unique_id }}', {# node_id #}
                '{{ run_started_at }}', {# run_started_at #}
                '{{ snapshot.database }}', {# database #}
                '{{ snapshot.schema }}', {# schema #}
                '{{ snapshot.name }}', {# name #}
                '{{ tojson(snapshot.depends_on.nodes) }}', {# depends_on_nodes #}
                '{{ snapshot.package_name }}', {# package_name #}
                '{{ snapshot.original_file_path | replace('\\', '\\\\') }}', {# path #}
                '{{ snapshot.checksum.checksum }}', {# checksum #}
                '{{ snapshot.config.strategy }}' {# strategy #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ snapshot_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

{% macro bigquery__get_snapshots_dml_sql(snapshots) -%}
    {% if snapshots != [] %}
        {% set snapshot_values %}
            {% for snapshot in snapshots -%}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ snapshot.unique_id }}', {# node_id #}
                    '{{ run_started_at }}', {# run_started_at #}
                    '{{ snapshot.database }}', {# database #}
                    '{{ snapshot.schema }}', {# schema #}
                    '{{ snapshot.name }}', {# name #}
                    {{ tojson(snapshot.depends_on.nodes) }}, {# depends_on_nodes #}
                    '{{ snapshot.package_name }}', {# package_name #}
                    '{{ snapshot.original_file_path | replace('\\', '\\\\') }}', {# path #}
                    '{{ snapshot.checksum.checksum }}', {# checksum #}
                    '{{ snapshot.config.strategy }}' {# strategy #}
                )
                {%- if not loop.last %},{%- endif %}
            {%- endfor %}
        {% endset %}
        {{ snapshot_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{%- endmacro %}
