{{ config(database="SPARC_RAW",schema="RBOK_GA") }}

EXECUTE flatten_GA4_events_raw()