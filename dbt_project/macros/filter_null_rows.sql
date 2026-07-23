{% macro filter_all_null_rows(columns) %}
    where not (
        {% for col in columns %}
            {{ col }} is null {% if not loop.last %}and {% endif %}
        {% endfor %}
    )
{% endmacro %}