{#
    A snapshot records the history of a table that changes slowly and overwrites itself.

    taxi_zone_lookup is a seed: when TLC renames a zone or moves it to another borough, the
    new CSV overwrites the old row and the old name is gone. This snapshot keeps both, so a
    2024 trip can still be labelled with the zone name that was correct in 2024.

    strategy = check: there is no "last updated" column to trust, so dbt compares the columns
    listed in check_cols and writes a new row whenever any of them differs.

    Each run adds dbt_valid_from / dbt_valid_to to every row. The current version has
    dbt_valid_to = null; older versions have the timestamp when they were replaced.
    This shape is called a type 2 slowly changing dimension.

    Run it with:  dbt snapshot     (dbt build runs snapshots too)
#}

{% snapshot snap_taxi_zones %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['zone', 'borough', 'service_zone'],
        invalidate_hard_deletes=true
    )
}}

select
    location_id,
    zone,
    borough,
    service_zone
from {{ ref('taxi_zone_lookup') }}

{% endsnapshot %}