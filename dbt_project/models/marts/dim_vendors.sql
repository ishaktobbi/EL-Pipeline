with seed_data as (
    select * from {{ ref('lkp_vendors') }}
)
select vendor_id, vendor_name from seed_data
union all
select -1 as vendor_id, 'Missing Vendor (System Null)' as vendor_name