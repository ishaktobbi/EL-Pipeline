with staging_data as (

    select * from {{ ref('stg_green_taxi_trips') }}


    WHERE 
    pickup_datetime IS NOT NULL 
    AND 
    dropoff_datetime IS NOT NULL
    AND 
    vendor_id IS NOT NULL
    AND
    total_amount IS NOT NULL
    AND 
    passenger_count IS NOT NULL
    AND
    trip_distance IS NOT NULL
    
),

deduplicated as (

    select
        *,
        -- Identify duplicates: if two rows have identical vendor, pickup, and dropoff, 
        -- the second one gets a row_num of 2.
        row_number() over (
            partition by pickup_datetime, dropoff_datetime, passenger_count, vendor_id
            order by total_amount desc
        ) as row_num

    from staging_data

),

cleaned as (

    select
        

        -- Handle missing business keys gracefully (Null Resolution)
        COALESCE(rate_code_id, -1) AS rate_code_id,
        COALESCE(pickup_location_id, -1) AS pickup_location_id,
        COALESCE(dropoff_location_id, -1) AS dropoff_location_id,
        COALESCE(payment_type_id, -1) AS payment_type_id,
        COALESCE(trip_type_id, -1) AS trip_type_id,

        pickup_datetime,
        dropoff_datetime,
        
        -- Clean structural dimensions
        vendor_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        congestion_surcharge,
        ehail_fee,
        partition_key,
        partition_date,

        -- Standardize Values to a common set of values (Data Standardization)
       
        -- -- 🟢 2. Pull the Clean Human-Readable Values from our Seeds
        -- coalesce(v.vendor_name, 'Unknown Vendor') as vendor_name,
        -- coalesce(r.rate_code, 'Standard Rate') as rate_code_desc,
        -- coalesce(p.payment_method, 'Unknown Payment') as payment_method




    from deduplicated

    where 
    
        ABS(DATE_DIFF(dropoff_datetime, partition_date, YEAR)) <= 1
        AND
        ABS(DATE_DIFF(pickup_datetime, partition_date, YEAR)) <= 1
        AND
            row_num = 1 -- 🟢 Drop the duplicates!
        AND total_amount >= 0 -- 🟢 Clean out negative fare anomalies
        AND passenger_count > 0 -- 🟢 Clean out data errors with 0 passengers
        AND trip_distance > 0 -- 🟢 Clean out data errors with 0 trip distance.

)

select * from cleaned