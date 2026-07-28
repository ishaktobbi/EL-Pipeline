-- models/marts/core/fct_taxi_trips.sql



    -- 🟢 INCREMENTAL CONFIGURATION NOTES:
    -- - unique_key: 
    --     The keys that makes a trip unique.

    -- - incremental_strategy: 
    --     Instructs BQ to update existing keys, acts as a join, lest is fct_ right is current ingestion.

    -- - CRITICAL POINT: 
    --     The current ingestion data must not have duplicates. 
    --     BQ will crash if it sees many records to compare fct_ existing record to. thus. we must make sure our ingestion data doesnt have duplicates.
    --     We handle this deduplication safely inside our intermediate/staging layer.
    
    -- - BQ partitioning: 
    --     The Physical Partition (Limits scanned data by date range)
    --     The Clusters (Sorts data inside the partitions for fast filtering)
      

    
{{config(
    materialized='incremental',

    unique_key=['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'service_type'], 
    
    incremental_strategy='merge',                                
    
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    
    cluster_by=['service_type', 'year_ingested', 'dropoff_datetime'],
    tags=['daily']
) }}



with yellow_taxi as (
    select * from {{ ref('int_yellow_taxi_trips') }}
),

green_taxi as (
    select * from {{ ref('int_green_taxi_trips') }}
),

-- Combine both datasets seamlessly
combined_trips as (
    -- the hash id created based on what we filtered duplicates in int_ model windowing + extra current run date and service.
    select 
        {{ dbt_utils.generate_surrogate_key(['vendor_id',
                                            'pickup_datetime',
                                            'dropoff_datetime',
                                            'passenger_count',
                                            '"yellow"']) }} as trip_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        rate_code_id,
        fare_amount,
        tip_amount,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        total_amount,
        year_ingested,
        month_ingested,
        'yellow' as service_type -- Tag the source for analysts
    from yellow_taxi

    union all

    select  
        {{ dbt_utils.generate_surrogate_key(['vendor_id',
                                            'pickup_datetime',
                                            'dropoff_datetime',
                                            'passenger_count',
                                            '"green"']) }} as trip_id,

        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        rate_code_id,
        fare_amount,
        tip_amount,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        total_amount,
        year_ingested,
        month_ingested,
        'green' as service_type
    from green_taxi
)
-- WE DONT NEED TO DEDUPLICATE HERE : IF OUR FCT_ DATA UNIQUE, AND INCOMING DATA HAS BEEN HANDLED IN STG_ AND IT IS UNIQUE, MERGE WILL ALWAYS WORKS.
-- deduplicated as (

--     select
--         *,

--     from combined_trips
--     where 
--         -- 🟢 Identify duplicates: if two rows have identical vendor, pickup, and dropoff, 
--         -- the second one gets a row_num of 2.
--         row_number() over (
--             partition by vendor_id, pickup_datetime, dropoff_datetime, service_type,
--             order by dropoff_datetime desc
--         ) = 1

-- ),

select * from combined_trips

{% if is_incremental() %}
 

  where 

-- Restrict the incremental scan strictly to the YEAR Airflow is currently processing, if a row exists, update it.
--  year_ingested = CAST('{{ var("run_year") }}' AS INT64)

-- Restrict the incremental scan strictly to >= +1 month OR <= +2 months of the Date Airflow is currently processing, if a row exists, update it.

    pickup_datetime >= DATE_SUB('{{ var("run_date") }}', INTERVAL 1 MONTH)
    AND
    dropoff_datetime <= DATE_ADD('{{ var("run_date") }}', INTERVAL 2 MONTH)
   


-- NOT USING IT : CHRONOLOGICAL FILTERING NOT USEFULL FOR DATA OUT OF ORDERS (2019 IN 2021, 0R 2021 IN 2019) SOME DATA LL BE LOST FOREVER.
--  -- 🟢 Only scan and grab rows newer than the max date already in the warehouse
--   where dropoff_datetime > coalesce(
--     -- we check if already exists current dropoff year, if yes, we ingest data right after its bigest year
--     (select max(dropoff_datetime) from {{ this }} where EXTRACT(year from dropoff_datetime) = CAST('{{ var("run_year") }}' AS INT64)),
--     -- if not, we select the min dropoff ear from our data = run year, and ingest from it.
--     (select min(dropoff_datetime) from combined_trips where EXTRACT(year from dropoff_datetime) = CAST('{{ var("run_year") }}' AS INT64)))
{% endif %}