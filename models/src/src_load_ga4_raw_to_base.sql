{{ config(materialized='table',database="SPARC_BASE",schema="ECOM_ANALYTICS") }}

select *
from {{ ref("src_execute_SP") }}
