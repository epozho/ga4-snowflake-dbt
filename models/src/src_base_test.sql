{{ config(materialized='table',schema="ECOM_ANALYTICS") }}

select *
from {{ ref("src_test") }}
limit 100
