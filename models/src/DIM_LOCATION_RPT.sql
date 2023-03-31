{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

Select * from SPARC_BASE.ECOM_ANALYTICS.DIM_LOCATION
WHERE BRAND_LOCATION_ID IN ('R0700','R0603','R6232','R6233','R6194')