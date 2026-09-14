-- A singular test: plain SQL that must return zero rows.
-- It fails if any trip's dropoff is at or before its pickup, which would mean a
-- negative or zero duration. clean_trips.py quarantines these, so this test proves
-- the silver rules are still doing their job.

select
    trip_id,
    pickup_at,
    dropoff_at,
    duration_min
from {{ ref('stg_trips') }}
where dropoff_at <= pickup_at