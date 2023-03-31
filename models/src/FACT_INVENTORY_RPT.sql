{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

Select * from SPARC_BASE.ECOM_ANALYTICS.FACT_INVENTORY