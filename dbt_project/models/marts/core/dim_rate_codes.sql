{{ config(
    tags=['static']
)}}

with seed_data as (
    select * from {{ ref('lkp_rate_codes') }}
)
select rate_code_id, rate_code from seed_data
union all
select -1 as rate_code_id, 'Missing Rate Code (System Null)' as rate_code