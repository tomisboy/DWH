{{
    config(
        materialized='table'
    )
}}

with w_ort_land as (
    SELECT ort.ort, land.land
    from staging.ort LEFT OUTER JOIN staging.land land on ort.land_id = land.land_id
)
select ROW_NUMBER () OVER () AS d_ort_key,
     c.land, 
     c.ort

from w_ort_land c