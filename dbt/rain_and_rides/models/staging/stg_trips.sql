-- One row per yellow taxi trip, straight from the Spark-loaded raw table.
-- Staging rule: rename and cast only, no joins and no business logic.

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        -- TLC gives trips no ID, so build one from the columns that together describe a trip
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_datetime', 'dropoff_datetime',
            'pu_location_id', 'do_location_id', 'total_amount'
        ]) }}                               as trip_id,

        -- when
        pickup_datetime                     as pickup_at,
        dropoff_datetime                    as dropoff_at,
        pickup_date,
        pickup_hour,
        duration_min,

        -- where
        pu_location_id                      as pickup_location_id,
        do_location_id                      as dropoff_location_id,
        is_airport_trip,

        -- who and how
        vendor_id,
        passenger_count,
        trip_distance                       as trip_distance_miles,
        ratecode_id,
        store_and_fwd_flag = 'Y'            as is_store_and_forward,
        payment_type,

        -- money (NUMERIC(12,2) in the warehouse: exact cents)
        fare_amount,
        extra                               as extra_amount,
        mta_tax,
        tip_amount,
        tip_pct,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        total_amount,

        -- lineage
        loaded_at

    from source

)

select * from renamed