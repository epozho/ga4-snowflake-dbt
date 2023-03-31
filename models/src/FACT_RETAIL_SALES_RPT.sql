{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

Select * from SPARC_BASE.ECOM_ANALYTICS.FACT_RETAIL_SALES
WHERE BRAND_LOCATION IN ('R0700','R0603','R6232','R6233','R6194')