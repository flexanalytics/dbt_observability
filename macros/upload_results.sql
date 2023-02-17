{# dbt doesn't like us ref'ing in an operation so we fetch the info from the graph #}
{% macro get_relation(get_relation_name) %}
    {% if execute %}
        {% set model_get_relation_node = graph.nodes.values() | selectattr('name', 'equalto', get_relation_name) | first %}
        {% set relation = api.Relation.create(
            database = model_get_relation_node.database,
            schema = model_get_relation_node.schema,
            identifier = model_get_relation_node.alias
        )
        %}
        {% do return(relation) %}
    {% else %}
        {% do return(api.Relation.create()) %}
    {% endif %}
{% endmacro %}

{% macro upload_results(results) -%}

    {% if execute %}

        {% do log("Uploading invocations", true) %}
        {% set invocations = dbt_observability.get_relation('invocations') %}
        {% set content_invocations = dbt_observability.upload_invocations() %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=invocations.database,
            schema_name=invocations.schema,
            table_name=invocations.identifier,
            content=content_invocations
            )
        }}

        {% if results != [] %}

            {% do log("Uploading all executions", true) %}
            {% set all_executions = dbt_observability.get_relation('all_executions') %}
            {% set content_all_executions = dbt_observability.upload_all_executions(results) %}
            {{ dbt_observability.insert_into_metadata_table(
                database_name=all_executions.database,
                schema_name=all_executions.schema,
                table_name=all_executions.identifier,
                content=content_all_executions
                )
            }}

        {% endif %}

        {% do log("Uploading models", true) %}
        {% set models = dbt_observability.get_relation('models') %}
        {% set content_models = dbt_observability.upload_models(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=models.database,
            schema_name=models.schema,
            table_name=models.identifier,
            content=content_models
            )
        }}

        {% do log("Uploading model columns", true) %}
        {% set models = dbt_observability.get_relation('columns') %}
        {% set content_columns = dbt_observability.upload_columns(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=models.database,
            schema_name=models.schema,
            table_name=models.identifier,
            content=content_columns
            )
        }}

        {% do log("Uploading exposures", true) %}
        {% set exposures = dbt_observability.get_relation('exposures') %}
        {% set content_exposures = dbt_observability.upload_exposures(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=exposures.database,
            schema_name=exposures.schema,
            table_name=exposures.identifier,
            content=content_exposures
            )
        }}

        {% do log("Uploading tests", true) %}
        {% set tests = dbt_observability.get_relation('tests') %}
        {% set content_tests = dbt_observability.upload_tests(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=tests.database,
            schema_name=tests.schema,
            table_name=tests.identifier,
            content=content_tests
            )
        }}

        {% do log("Uploading seeds", true) %}
        {% set seeds = dbt_observability.get_relation('seeds') %}
        {% set content_seeds = dbt_observability.upload_seeds(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=seeds.database,
            schema_name=seeds.schema,
            table_name=seeds.identifier,
            content=content_seeds
            )
        }}

        {% do log("Uploading sources", true) %}
        {% set sources = dbt_observability.get_relation('sources') %}
        {% set content_sources = dbt_observability.upload_sources(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=sources.database,
            schema_name=sources.schema,
            table_name=sources.identifier,
            content=content_sources
            )
        }}

        {% do log("Uploading snapshots", true) %}
        {% set snapshots = dbt_observability.get_relation('snapshots') %}
        {% set content_snapshots = dbt_observability.upload_snapshots(graph) %}
        {{ dbt_observability.insert_into_metadata_table(
            database_name=snapshots.database,
            schema_name=snapshots.schema,
            table_name=snapshots.identifier,
            content=content_snapshots
            )
        }}

    {% endif %}
{%- endmacro %}
