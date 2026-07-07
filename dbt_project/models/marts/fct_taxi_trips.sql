-- models/marts/core/fct_taxi_trips.sql

with yellow_taxi as (
    select * from {{ ref('int_yellow_taxi_trips') }}
),

green_taxi as (
    select * from {{ ref('int_green_taxi_trips') }}
),

-- Combine both datasets seamlessly
combined_trips as (
    select 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        total_amount,
        'yellow' as service_type -- Tag the source for analysts
    from yellow_taxi

    union all

    select 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        total_amount,
        'green' as service_type
    from green_taxi
)

select * from combined_trips

{% if is_incremental() %}
  -- 🟢 Only scan and grab rows newer than the max date already in the warehouse
  where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}