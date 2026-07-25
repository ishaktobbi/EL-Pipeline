{% macro test_accepted_int_values(model, column_name, values) %} 

{# 🟢 1. Added test_ prefix so dbt look at it as a test  #} 

with validation_errors as (
    select
        {{ column_name }} as test_field {# 🟢 dbt automatically passes the target column name view/table here #}
    from {{ model }} {# 🟢 dbt automatically passes the target model view/table here #}
)

select *
from validation_errors
where not (     {# 🟢 test is to grab the opposite : for values that are NOT in the accepted list. #}
    test_field in (
        {% for value in values %}
            {{ value }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    )
)

{% endmacro %}
{# {% if not loop.last %}, {% endif %}  this is only to print comma , if the item is not the last one. #}