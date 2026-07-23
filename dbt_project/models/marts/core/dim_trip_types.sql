{{ config(
    materialized='table',
    tags=['static']
)}}

with seed_data as (
    select * from {{ ref('lkp_trip_types') }}
)
select trip_type_id, trip_type from seed_data
union all
select -1 as trip_type_id, 'Missing Trip Type (System Null)' as trip_type