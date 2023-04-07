{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

Select
ORIGINAL_TRANSACTION_ID,
TRANSACTION_DATE,
sum(UNITS_SOLD) as UNITS_SOLD,
sum(GROSS_SALES_USD) as GROSS_SALES_USD,
sum(COST_USD) as COST_USD,
(sum(GROSS_SALES_USD)-sum(COST_USD)) as MARGIN_SALES,
(CASE WHEN DISCOUNT_USD>0 THEN 1 ELSE 0 END) as DISCOUNTED_FLAG,
sum(PROMO_MARKDOWN_USD) as PROMO_MARKDOWN_USD,
sum(TOTAL_MARKDOWNS_USD) as TOTAL_MARKDOWNS_USD,
sum(DISCOUNT_USD) as DISCOUNT_USD,
count(distinct ORIGINAL_TRANSACTION_ID) as NUMBER_OF_ORDERS,
TRANSACTION_TYPE_CODE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
DIVISION_NAME,
STYLE_NAME,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
BUSINESS_SEGMENT,
KEY_CATEGORY,
SPARC_LOCATION_ID,
BRAND_LOCATION_ID,
LOCATION_NAME,
SUB_CHANNEL_ID,
SUB_CHANNEL,
CHANNEL_ID,
CHANNEL,
CITY,
STATE,
POSTAL_CODE,
COUNTRY,
LATITUDE,
LONGITUDE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM
from
(
Select
Sales.ORIGINAL_TRANSACTION_ID
Sales.TRANSACTION_DATE,
Sales.UNITS_SOLD,
Sales.GROSS_SALES_USD,
Sales.TRANSACTION_TYPE_CODE,
Sales.PROMO_MARKDOWN_USD,
Sales.TOTAL_MARKDOWNS_USD,
Sales.COST_USD,
Sales.DISCOUNT_USD,
Product.ITEM_SEASON_NAME,
Product.PRODUCT_GROUP,
Product.ARTICLE_CREATION_SEASON,
Product.ARTICLE_ACTIVE_SEASON,
Product.DIVISION_NAME,
Product.STYLE_NAME,
Product.MODEL_NUMBER,
Product.VENDOR_ID,
Product.FORECAST_IND,
Product.ORIGINAL_RETAIL,
Product.BUSINESS_SEGMENT,
Product.KEY_CATEGORY,
Location.SPARC_LOCATION_ID,
Location.BRAND_LOCATION_ID,
Location.LOCATION_NAME,
Location.SUB_CHANNEL_ID,
Location.SUB_CHANNEL,
Location.CHANNEL_ID,
Location.CHANNEL,
Location.CITY,
Location.STATE,
Location.POSTAL_CODE,
Location.COUNTRY,
Location.LATITUDE,
Location.LONGITUDE,
Date.CALDT as CALDT,
Quarter.ACCTGPRDQTRNUM as ACCTGPRDQTRNUM,
Quarter.ACCTGPRDQTRLNGNM as ACCTGPRDQTRLNGNM,
Date.ACCTGPRDYRNM,
SUBSTR(Date.ACCTGPRDYRNM,6,4) as ACCTGPRDYR,
Quarter.ACCTGPRDQTRNM as ACCTGPRDQTRNM,
Date.ACCTGPRDMNTHNM,
Date.ACCTGPRDWOYNM,
SUBSTR(Date.ACCTGPRDWOYNM,6,2) as ACCTGPRDWOYNM_NUMBER,
Date.SEASNNM as SEASNNM
FROM "SPARC_BASE"."ECOM_ANALYTICS"."FACT_RETAIL_SALES" as Sales
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_PRODUCT" as Product on Product.ITEM_PK=Sales.ITEM_PK
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_LOCATION" as Location on Location.LOCATION_PK=Sales.LOCATION_PK
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_DATE" as Date on Date.CALDT=Sales.TRANSACTION_DATE
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_QUARTER" as Quarter on Quarter.ACCTGPRDQTRID=Date.ACCTGPRDQTRID
WHERE Sales.TRANSACTION_DATE > '2023-01-01'
)
GROUP BY
ORIGINAL_TRANSACTION_ID
TRANSACTION_DATE,
TRANSACTION_TYPE_CODE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
DIVISION_NAME,
STYLE_NAME,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
BUSINESS_SEGMENT,
KEY_CATEGORY,
DISCOUNTED_FLAG,
SPARC_LOCATION_ID,
BRAND_LOCATION_ID,
LOCATION_NAME,
SUB_CHANNEL_ID,
SUB_CHANNEL,
CHANNEL_ID,
CHANNEL,
CITY,
STATE,
POSTAL_CODE,
COUNTRY,
LATITUDE,
LONGITUDE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM