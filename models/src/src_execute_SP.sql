{{ config(materialized='table',database="SPARC_BASE",schema="ECOM_ANALYTICS") }}


-- {% set my_proc_call = "CALL flatten_GA4_events_raw_sample() "%}
-- {% set my_proc_results = run_operation(my_proc_call) %}

-- Select * from {{ ref('my_proc_results') }}

Select * from 
from {{ ref("src_test") }}
