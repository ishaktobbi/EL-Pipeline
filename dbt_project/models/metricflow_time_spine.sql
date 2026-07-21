{{ config(materialized='table') }}

select date_day
from unnest(
    generate_date_array(date('2010-01-01'), date('2030-01-01'), interval 1 day)
) as date_day