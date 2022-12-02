{% snapshot d_kunde_snapshot %}

{{
    config(
        target_schema='staging',
        unique_key='kunde_id',

        strategy='timestamp',
        updated_at='erstellt_am',
    )
}}

with w_kunde as (
    select sk.kunde_id, sk.vorname, sk.nachname, sk.anrede, sk.geschlecht, sk.geburtsdatum, so.ort, sl.land, sk.erstellt_am from {{ source('staging', 'kunde')}} as sk
    inner join {{ source('staging', 'ort')}} as so on sk.wohnort = so.ort_id
    inner join {{ source('staging', 'land')}} as sl on so.land_id = sl.land_id
)

select o.kunde_id, o.vorname, o.nachname, o.anrede, o.geschlecht, o.geburtsdatum, o.ort, o.land, o.erstellt_am
from w_kunde o

{% endsnapshot %}