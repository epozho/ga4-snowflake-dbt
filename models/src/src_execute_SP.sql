{{ config(materialized='table',database="SPARC_RAW",schema="RBOK_GA") }}

Select *
FROM TABLE(EXECUTE flatten_GA4_events_raw())