with staging_data as (

    select * from {{ ref('stg_green_taxi_trips') }}

    WHERE 
    pickup_datetime IS NOT NULL 
    OR 
    dropoff_datetime IS NOT NULL
    OR
    total_amount IS NOT NULL
    OR
    passenger_count IS NOT NULL
    OR
    trip_distance IS NOT NULL  
    
    -- 🟢 Purges the record from entering the marts

),

deduplicated as (

    select
        *,
        -- 🟢 Identify duplicates: if two rows have identical vendor, pickup, and dropoff, 
        -- the second one gets a row_num of 2.
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime 
            order by total_amount desc
        ) as row_num

    from staging_data

),

cleaned as (

    select
        

        -- Handle missing business keys gracefully (Null Resolution)
        COALESCE(vendor_id, -1) AS vendor_id,
        COALESCE(rate_code_id, -1) AS rate_code_id,
        COALESCE(pickup_location_id, -1) AS pickup_location_id,
        COALESCE(dropoff_location_id, -1) AS dropoff_location_id,
        COALESCE(payment_type_id, -1) AS payment_type_id,
        COALESCE(trip_type_id, -1) AS trip_type_id,

        pickup_datetime,
        dropoff_datetime,
        
        -- Clean structural dimensions
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        congestion_surcharge,
        ehail_fee,


        -- Standardize Values to a common set of values (Data Standardization)
       
        -- -- 🟢 2. Pull the Clean Human-Readable Values from our Seeds
        -- coalesce(v.vendor_name, 'Unknown Vendor') as vendor_name,
        -- coalesce(r.rate_code, 'Standard Rate') as rate_code_desc,
        -- coalesce(p.payment_method, 'Unknown Payment') as payment_method




    from deduplicated


    -- -- 🟢 The Seed Joins
    -- left join {{ ref('lkp_vendors') }} v 
    --     on vendor_id = v.vendor_id
    -- left join {{ ref('lkp_rate_codes') }} r 
    --     on rate_code_id = r.rate_code_id
    -- left join {{ ref('lkp_payment_methods') }} p 
    --     on payment_type_id = p.payment_type_id


    where row_num = 1 -- 🟢 Drop the duplicates!
      and total_amount >= 0 -- 🟢 Clean out negative fare anomalies
      and passenger_count > 0 -- 🟢 Clean out data errors with 0 passengers
      and trip_distance > 0 -- 🟢 Clean out data errors with 0 trip distance.

)

select * from cleaned