-- One row per holiday listed by Nager.Date, nationwide and state-level.

with source as (

    select * from {{ source('raw', 'holidays') }}

),

renamed as (

    select
        holiday_date,
        name                                as holiday_name,
        local_name                          as holiday_local_name,
        is_global                           as is_nationwide,
        counties                            as observed_in,  -- e.g. 'US-CT,US-IL,US-NY', null when nationwide
        types                               as holiday_types,
        loaded_at

    from source

)

select * from renamed