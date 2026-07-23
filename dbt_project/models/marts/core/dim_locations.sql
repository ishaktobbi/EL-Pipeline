{{ config(
    materialized='table',
    tags=['static']
)}}

with seed_data as (
    select * from {{ ref('lkp_locations') }}
)
select 
    location_id,
    borough,
    zone,
    service_zone
 from seed_data

union all

select -1 as location_id, 
        'Missing Borough (System Null)' as borough,
        'Missing zone (System Null)' as zone,
        'Missing service (System Null)' as service_zone