{% test taxi_consistent(model) %}

{# This test returns rows that FAIL the assertion.  #} 
{# If it returns 0 rows, the test passes! #} 

select *
from {{ model }}
where total_amount < 0 
   or total_amount is null
   or passenger_count <= 0
   or passenger_count is null

{% endtest %}