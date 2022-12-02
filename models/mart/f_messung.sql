{{
    config(
        materialized='view'
    )
}}


with w as (
select def.kunde_id, payload ->> 'fin' as fin, payload ->> 'zeit' as messung_eingetroffen, 
CAST((payload ->> 'geschwindigkeit')AS int) as geschwindigkeit, payload ->> 'ort' as Ort, messung.erstellt_am as messung_erzeugt
from staging.messung 
inner join staging.fahrzeug as abc on abc.fin = fin
inner join staging.kunde as def on def.kunde_id = abc.kunde_id




)

select *
from w c
