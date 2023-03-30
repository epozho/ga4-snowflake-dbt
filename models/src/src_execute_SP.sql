{{ config(
    materialized="table",
    database="SPARC_BASE",
    schema="ECOM_ANALYTICS"
)}}

Select * 
from CALL flatten_GA4_events_raw_sample();
