{{ config(materialized="table", schema="RBOK_GA") }}

select *
from {{ source("SPARC_RAW", "ga4_table") }}
limit 100
