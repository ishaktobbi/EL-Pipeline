{{ config(
    tags=['static']
)}}

with seed_data as (
    select * from {{ ref('lkp_payment_methods') }}
)
select payment_type_id, payment_method from seed_data
union all
select -1 as payment_type_id, 'Missing Payment (System Null)' as payment_method