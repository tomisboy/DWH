{{
    config(
        materialized='incremental'
    )
}}

with w as (

        SELECT 
            fahrzeug.fin,
            kfz.kfz_kennzeichen,
            baujahr,
            modell,
            hersteller.hersteller
        FROM (staging.fahrzeug
            LEFT OUTER JOIN staging.hersteller hersteller on fahrzeug.hersteller_code = hersteller.hersteller_code)
                 LEFT OUTER JOIN staging.kfzzuordnung kfz on fahrzeug.fin = kfz.fin
                 
)
select ROW_NUMBER () OVER () AS d_fahrzeug_key, 
    c.fin,
    c.kfz_kennzeichen,
    c.baujahr,
    c.modell,
    c.hersteller
from w c