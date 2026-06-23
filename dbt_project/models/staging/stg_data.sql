with raw_source as (
    -- This function dynamically compiles to your real BigQuery path
    select * from {{ source('gcp_raw_lake', 'external_yellow_tripdata') }}
),

renamed_and_cleaned as (
    select
        -- 1. Standardize your column naming conventions
        payment_type,
        VendorID as Vendor_id,
        passenger_count as passenger_number,
        
        -- 2. Clean up data types (explicit casting)
        -- cast(event_timestamp as timestamp) as occurred_at,
        -- lower(event_type) as event_name,
        
        -- 3. Handle null values safely
        coalesce(passenger_count, 0) as column_name_with_no_nulls
        
    from raw_source
    where VendorID is not null
)

select * from renamed_and_cleaned