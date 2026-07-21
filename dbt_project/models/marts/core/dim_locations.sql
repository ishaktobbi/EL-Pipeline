{{ config(
    tags=['static']
)}}

with seed_data as (
    select * from {{ ref('lkp_locations') }}
)
select location_id,location_borough,location_zone,service_zone
 from seed_data

union all

select -1 as location_id, 
        'Missing Borough (System Null)' as location_borough,
        'Missing zone (System Null)' as location_zone,
        'Missing service (System Null)' as service_zone,