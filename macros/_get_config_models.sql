{% macro get_models_list(graph, path=None, materialization=[]) %}
    {% set models = [] %}
        {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("package_name", "equalto", project_name) %}
            {% if path and not materialization %}
                {% if (path + node.name + ".sql") == node.original_file_path %}
                    {% do models.append(node) %}
                {% endif %}
            {% elif not path and materialization %}
                {% if node.config.materialized in materialization %}
                    {% do models.append(node) %}
                {% endif %}
            {% elif path %}
                {% if (path + node.name + ".sql") == node.original_file_path and node.config.materialized in materialization %}
                    {% do models.append(node) %}
                {% endif %}
            {% else %}
                {% do models.append(node) %}
            {% endif %}
        {% endfor %}
    {{ return(models) }}
{% endmacro %}
