{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

Select * from (
SELECT
--Common
'RETAIL_SALES' as SOURCE,
CONCAT(DATEDIFF(day, '1900-01-01'::TIMESTAMP, TRANSACTION_DATE)+2,'|',BRAND_LOCATION_ID) as BUSINESS_PERFORMANCE_DATE_LOCATION_KEY,
CONCAT(ACCTGPRDMNTHNUM,'|',KEY_CATEGORY_DESC) as PRODUCT_DETAIL_STYLE_MONTH_KEY,
TRANSACTION_DATE,
DIVISION_NAME,
--STYLE_NAME,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE_DESC,
ARTICLE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
--Retail
ORIGINAL_TRANSACTION_ID,
UNITS_SOLD,
GROSS_SALES_USD,
EVEN_EXCHANGE_FLAG,
COST_USD,
MARGIN_SALES,
DISCOUNTED_FLAG,
PROMO_MARKDOWN_USD,
TOTAL_MARKDOWNS_USD,
DISCOUNT_USD,
SELLING_CHANNEL_ID,
ORDER_TYPE_ID,
STARTING_PRICE,
PRICE_TYPE,
NUMBER_OF_ORDERS,
TRANSACTION_TYPE_CODE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
upper(STYLE_NAME) as STYLE_NAME,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
SPARC_LOCATION_ID,
BRAND_LOCATION_ID,
(CASE
    WHEN BRAND_LOCATION_ID='R0700' THEN 'Reebok'
    WHEN BRAND_LOCATION_ID IN ('R0603','R6232','R6233','R6194') THEN 'Marketplace'
    WHEN BRAND_LOCATION_ID is NULL THEN 'NULL'
    ELSE 'Retail Sales'
END) AS SALES_CHANNEL,
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
--GA4
NULL AS USER_SESSION_KEY,
NULL AS MAX_CHECKOUT_STAGE,
NULL AS ECOMMERCE_TRANSACTION_ID,
NULL AS DEVICE_CATEGORY,
NULL AS UNITS_ORDERED,
NULL AS FRANCHISE,
NULL AS GENDER,
--NULL AS CORPORATE_MARKETING_LINE,
--NULL AS PRODUCT_TYPE,
--NULL AS CATEGORY_MARKETING_LINE,
NULL AS ITEMS_ITEM_REVENUE_IN_USD,
NULL AS LASTTOUCH_TRAFFIC_CAMPAIGN,
NULL AS LASTTOUCH_TRAFFIC_MEDIUM,
NULL AS LASTTOUCH_TRAFFIC_SOURCE,
NULL AS MEDIA_CHANNEL,
NULL as GA_MEDIA_CHANNEL_MONTH_KEY,
NULL AS FUNNEL,
--Inventory
--BRAND_CODE,
--LOCATION_PK,
--ITEM_PK,
--BRAND_ITEM,
NULL AS ON_HAND_UNITS,
--IN_TRANSIT_UNITS,
--ON_ORDER_UNITS,
--TOTAL_ON_HAND_UNITS,
NULL AS ON_HAND_DOLLARS_USD,
--IN_TRANSIT_DOLLARS_USD,
--ON_ORDER_DOLLARS_USD,
--TOTAL_ON_HAND_DOLLARS_USD,
--MAO
NULL AS ORDER_ID,
--NULL AS MIN_FULFILLMENT_STATUS_ID,
NULL AS MAX_FULFILLMENT_STATUS_ID,
NULL AS CURRENT_STATUS,
NULL AS OPEN_ORDER_ID,
NULL AS IS_RETURN,
NULL AS IS_CANCELLED,
NULL AS DEMAND_UNITS,
NULL AS DEMAND_VENDOR_COST_USD,
-- NULL AS DEMAND_AVERAGE_COST_USD,
NULL AS DEMAND_TOTAL_COST,
NULL AS DEMAND_DOLLARS_USD,
NULL AS DISCOUNT_AMOUNT_USD,
NULL AS FULFILLMENT_ID
from "RBOK_RPT"."ECOM_ANALYTICS"."RETAIL_SALES_AGGREGATED"
)

UNION

Select * from(
SELECT
--Common
'GA4' as SOURCE,
CONCAT(DATEDIFF(day, '1900-01-01'::TIMESTAMP, EVENT_DATE)+2,'|','R0700') as BUSINESS_PERFORMANCE_DATE_LOCATION_KEY,
CONCAT(ACCTGPRDMNTHNUM,'|',KEY_CATEGORY_DESC) as PRODUCT_DETAIL_STYLE_MONTH_KEY,
EVENT_DATE as TRANSACTION_DATE,
DIVISION_NAME,
--STYLE as STYLE_NAME,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE as FRANCHISE_DESC,
ARTICLE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
--Retail
NULL AS ORIGINAL_TRANSACTION_ID,
NULL AS UNITS_SOLD,
NULL AS GROSS_SALES_USD,
NULL AS EVEN_EXCHANGE_FLAG,
NULL AS COST_USD,
NULL AS MARGIN_SALES,
NULL AS DISCOUNTED_FLAG,
NULL AS PROMO_MARKDOWN_USD,
NULL AS TOTAL_MARKDOWNS_USD,
NULL AS DISCOUNT_USD,
NULL AS SELLING_CHANNEL_ID,
NULL AS ORDER_TYPE_ID,
NULL AS STARTING_PRICE,
NULL AS PRICE_TYPE,
NULL AS NUMBER_OF_ORDERS,
NULL AS TRANSACTION_TYPE_CODE,
NULL AS ITEM_SEASON_NAME,
NULL AS PRODUCT_GROUP,
NULL AS ARTICLE_CREATION_SEASON,
NULL AS ARTICLE_ACTIVE_SEASON,
NULL AS STYLE_NAME,
NULL AS VENDOR_ID,
NULL AS FORECAST_IND,
NULL AS ORIGINAL_RETAIL,
NULL AS SPARC_LOCATION_ID,
'R0700' AS BRAND_LOCATION_ID,
(CASE
    WHEN BRAND_LOCATION_ID='R0700' THEN 'Reebok'
    WHEN BRAND_LOCATION_ID IN ('R0603','R6232','R6233','R6194') THEN 'Marketplace'
    WHEN BRAND_LOCATION_ID is NULL THEN 'NULL'
    ELSE 'Retail Sales'
END) AS SALES_CHANNEL,
NULL AS LOCATION_NAME,
NULL AS SUB_CHANNEL_ID,
NULL AS SUB_CHANNEL,
NULL AS CHANNEL_ID,
NULL AS CHANNEL,
NULL AS CITY,
NULL AS STATE,
NULL AS POSTAL_CODE,
NULL AS COUNTRY,
NULL AS LATITUDE,
NULL AS LONGITUDE,
--GA4
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
UNITS_ORDERED,
FRANCHISE,
GENDER,
--CORPORATE_MARKETING_LINE,
--PRODUCT_TYPE,
--CATEGORY_MARKETING_LINE,
ITEMS_ITEM_REVENUE_IN_USD,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
CONCAT(ACCTGPRDMNTHNUM,'|',MEDIA_CHANNEL) as GA_MEDIA_CHANNEL_MONTH_KEY,
FUNNEL,
--Inventory
--BRAND_CODE,
--LOCATION_PK,
--ITEM_PK,
--BRAND_ITEM,
NULL AS ON_HAND_UNITS,
--IN_TRANSIT_UNITS,
--ON_ORDER_UNITS,
--TOTAL_ON_HAND_UNITS,
NULL AS ON_HAND_DOLLARS_USD,
--IN_TRANSIT_DOLLARS_USD,
--ON_ORDER_DOLLARS_USD,
--TOTAL_ON_HAND_DOLLARS_USD,
--MAO
NULL AS ORDER_ID,
--NULL AS MIN_FULFILLMENT_STATUS_ID,
NULL AS MAX_FULFILLMENT_STATUS_ID,
NULL AS CURRENT_STATUS,
NULL AS OPEN_ORDER_ID,
NULL AS IS_RETURN,
NULL AS IS_CANCELLED,
NULL AS DEMAND_UNITS,
NULL AS DEMAND_VENDOR_COST_USD,
-- NULL AS DEMAND_AVERAGE_COST_USD,
NULL AS DEMAND_TOTAL_COST,
NULL AS DEMAND_DOLLARS_USD,
NULL AS DISCOUNT_AMOUNT_USD,
NULL AS FULFILLMENT_ID
from "RBOK_RPT"."ECOM_ANALYTICS"."GA4_AGGREGATED"
)

UNION

Select * from(
Select
--Common
'INVENTORY' as SOURCE,
NULL AS BUSINESS_PERFORMANCE_DATE_LOCATION_KEY,
NULL AS PRODUCT_DETAIL_STYLE_MONTH_KEY,
INVENTORY_DATE AS TRANSACTION_DATE,
DIVISION_NAME,
--STYLE as STYLE_NAME,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE_DESC,
ARTICLE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
--Retail
NULL AS ORIGINAL_TRANSACTION_ID,
NULL AS UNITS_SOLD,
NULL AS GROSS_SALES_USD,
NULL AS EVEN_EXCHANGE_FLAG,
NULL AS COST_USD,
NULL AS MARGIN_SALES,
NULL AS DISCOUNTED_FLAG,
NULL AS PROMO_MARKDOWN_USD,
NULL AS TOTAL_MARKDOWNS_USD,
NULL AS DISCOUNT_USD,
NULL AS SELLING_CHANNEL_ID,
NULL AS ORDER_TYPE_ID,
NULL AS STARTING_PRICE,
NULL AS PRICE_TYPE,
NULL AS NUMBER_OF_ORDERS,
NULL AS TRANSACTION_TYPE_CODE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
upper(STYLE_NAME) as STYLE_NAME,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
SPARC_LOCATION_ID,
BRAND_LOCATION as BRAND_LOCATION_ID,
(CASE
    WHEN BRAND_LOCATION='R0700' THEN 'Reebok'
    WHEN BRAND_LOCATION IN ('R0603','R6232','R6233','R6194') THEN 'Marketplace'
    WHEN BRAND_LOCATION is NULL THEN 'NULL'
    ELSE 'Retail Sales'
END) AS SALES_CHANNEL,
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
--GA4
NULL AS USER_SESSION_KEY,
NULL AS MAX_CHECKOUT_STAGE,
NULL AS ECOMMERCE_TRANSACTION_ID,
NULL AS DEVICE_CATEGORY,
NULL AS UNITS_ORDERED,
NULL AS FRANCHISE,
NULL AS GENDER,
--NULL AS CORPORATE_MARKETING_LINE,
--NULL AS PRODUCT_TYPE,
--NULL AS CATEGORY_MARKETING_LINE,
NULL AS ITEMS_ITEM_REVENUE_IN_USD,
NULL AS LASTTOUCH_TRAFFIC_CAMPAIGN,
NULL AS LASTTOUCH_TRAFFIC_MEDIUM,
NULL AS LASTTOUCH_TRAFFIC_SOURCE,
NULL AS MEDIA_CHANNEL,
NULL as GA_MEDIA_CHANNEL_MONTH_KEY,
NULL AS FUNNEL,
--Inventory
--BRAND_CODE,
--LOCATION_PK,
--ITEM_PK,
--BRAND_ITEM,
ON_HAND_UNITS,
--IN_TRANSIT_UNITS,
--ON_ORDER_UNITS,
--TOTAL_ON_HAND_UNITS,
ON_HAND_DOLLARS_USD,
--IN_TRANSIT_DOLLARS_USD,
--ON_ORDER_DOLLARS_USD,
--TOTAL_ON_HAND_DOLLARS_USD,
--MAO
NULL AS ORDER_ID,
--NULL AS MIN_FULFILLMENT_STATUS_ID,
NULL AS MAX_FULFILLMENT_STATUS_ID,
NULL AS CURRENT_STATUS,
NULL AS OPEN_ORDER_ID,
NULL AS IS_RETURN,
NULL AS IS_CANCELLED,
NULL AS DEMAND_UNITS,
NULL AS DEMAND_VENDOR_COST_USD,
-- NULL AS DEMAND_AVERAGE_COST_USD,
NULL AS DEMAND_TOTAL_COST,
NULL AS DEMAND_DOLLARS_USD,
NULL AS DISCOUNT_AMOUNT_USD,
NULL AS FULFILLMENT_ID
from "RBOK_RPT"."ECOM_ANALYTICS"."INVENTORY_AGGREGATED"
)

UNION

Select * from(
Select
--Common
SOURCE as SOURCE,
CONCAT(DATEDIFF(day, '1900-01-01'::TIMESTAMP, DEMAND_DATE_LOCAL)+2,'|','R0700') AS BUSINESS_PERFORMANCE_DATE_LOCATION_KEY,
CONCAT(ACCTGPRDMNTHNUM,'|',KEY_CATEGORY_DESC) AS PRODUCT_DETAIL_STYLE_MONTH_KEY,
DEMAND_DATE_LOCAL AS TRANSACTION_DATE,
DIVISION_NAME,
--STYLE as STYLE_NAME,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE_DESC,
ARTICLE,
CALDT,
ACCTGPRDQTRNUM,
ACCTGPRDQTRLNGNM,
ACCTGPRDYRNM,
ACCTGPRDYR,
ACCTGPRDQTRNM,
ACCTGPRDMNTHNM,
ACCTGPRDWOYNM,
ACCTGPRDWOYNM_NUMBER,
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
--Retail
NULL AS ORIGINAL_TRANSACTION_ID,
NULL AS UNITS_SOLD,
NULL AS GROSS_SALES_USD,
NULL AS EVEN_EXCHANGE_FLAG,
NULL AS COST_USD,
NULL AS MARGIN_SALES,
NULL AS DISCOUNTED_FLAG,
NULL AS PROMO_MARKDOWN_USD,
NULL AS TOTAL_MARKDOWNS_USD,
NULL AS DISCOUNT_USD,
NULL AS SELLING_CHANNEL_ID,
ORDER_TYPE AS ORDER_TYPE_ID,
NULL AS STARTING_PRICE,
PRICE_TYPE AS PRICE_TYPE,
NULL AS NUMBER_OF_ORDERS,
NULL AS TRANSACTION_TYPE_CODE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
upper(STYLE_NAME) as STYLE_NAME,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
SPARC_LOCATION_ID,
BRAND_LOCATION as BRAND_LOCATION_ID,
(CASE
    WHEN BRAND_LOCATION='R0700' THEN 'Reebok'
    WHEN BRAND_LOCATION IN ('R0603','R6232','R6233','R6194') THEN 'Marketplace'
    WHEN BRAND_LOCATION is NULL THEN 'NULL'
    ELSE 'Retail Sales'
END) AS SALES_CHANNEL,
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
--GA4
NULL AS USER_SESSION_KEY,
NULL AS MAX_CHECKOUT_STAGE,
NULL AS ECOMMERCE_TRANSACTION_ID,
NULL AS DEVICE_CATEGORY,
NULL AS UNITS_ORDERED,
NULL AS FRANCHISE,
NULL AS GENDER,
--NULL AS CORPORATE_MARKETING_LINE,
--NULL AS PRODUCT_TYPE,
--NULL AS CATEGORY_MARKETING_LINE,
NULL AS ITEMS_ITEM_REVENUE_IN_USD,
NULL AS LASTTOUCH_TRAFFIC_CAMPAIGN,
NULL AS LASTTOUCH_TRAFFIC_MEDIUM,
NULL AS LASTTOUCH_TRAFFIC_SOURCE,
NULL AS MEDIA_CHANNEL,
NULL as GA_MEDIA_CHANNEL_MONTH_KEY,
NULL AS FUNNEL,
--Inventory
--BRAND_CODE,
--LOCATION_PK,
--ITEM_PK,
--BRAND_ITEM,
NULL AS ON_HAND_UNITS,
--IN_TRANSIT_UNITS,
--ON_ORDER_UNITS,
--TOTAL_ON_HAND_UNITS,
NULL AS ON_HAND_DOLLARS_USD,
--IN_TRANSIT_DOLLARS_USD,
--ON_ORDER_DOLLARS_USD,
--TOTAL_ON_HAND_DOLLARS_USD,
--MAO
ORDER_ID,
--MIN_FULFILLMENT_STATUS_ID,
MAX_FULFILLMENT_STATUS_ID,
CURRENT_STATUS,
OPEN_ORDER_ID,
IS_RETURN,
IS_CANCELLED,
DEMAND_UNITS,
DEMAND_VENDOR_COST_USD,
-- DEMAND_AVERAGE_COST_USD,
DEMAND_TOTAL_COST,
DEMAND_DOLLARS_USD,
DISCOUNT_AMOUNT_USD,
FULFILLMENT_ID
from "RBOK_RPT"."ECOM_ANALYTICS"."MAO_AGGREGATED"
)
