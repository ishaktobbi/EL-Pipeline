with clean_staging_events as (
    -- Tells dbt: Wait for stg_data to finish building, then read from it!
    select * from {{ ref('stg_data') }}
),

final_dimension as (
    select
        payment_type,
        count(vendor_id) as total_interactions,
        -- min(occurred_at) as first_seen_at,
        -- max(occurred_at) as last_active_at
        
    from clean_staging_events
    group by 1
)

select * from final_dimension