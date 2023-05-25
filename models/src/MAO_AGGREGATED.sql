{{ config(materialized="table", database="RBOK_RPT", schema="ECOM_ANALYTICS") }}

--Created a virtual table for Dates to bring in necessary calculations across Week, Quarter and Month
With 
Date_Table_Calc as
(
Select
Date.CALDT AS CALDT,
Date.ACCTGPRDMNTHID,
Date.ACCTGPRDQTRID,
Date.ACCTGPRDYRID,
Quarter.ACCTGPRDQTRNUM AS ACCTGPRDQTRNUM,
Quarter.ACCTGPRDQTRLNGNM AS ACCTGPRDQTRLNGNM,
Date.ACCTGPRDYRNM,
SUBSTR(Date.ACCTGPRDYRNM,6,4) AS ACCTGPRDYR,
Quarter.ACCTGPRDQTRNM AS ACCTGPRDQTRNM,
Date.ACCTGPRDMNTHNM,
Date.ACCTGPRDWOYNM,
SUBSTR(Date.ACCTGPRDWOYNM,6,2) AS ACCTGPRDWOYNM_NUMBER,
Date.SEASNNM AS SEASNNM,
Week.ACCTGPRDYRWKNUM AS ACCTGPRDYRWKNUM,
        CONCAT(
	        SUBSTR(Date.ACCTGPRDYRNM,6,4), 
	        (CASE 
	            WHEN LENGTH(Month.ACCTGPRDMNTHNUM)=1 
		        THEN CONCAT('0', TO_VARCHAR(Month.ACCTGPRDMNTHNUM)) 
	            ELSE TO_VARCHAR(Month.ACCTGPRDMNTHNUM) 
            END)
        )  AS ACCTGPRDMNTHNUM,
CONCAT(SUBSTR(Date.ACCTGPRDYRNM,6,4),' ',Date.ACCTGPRDMNTHNM) AS MONTH_LONG_NM,
CONCAT(SUBSTR(Date.ACCTGPRDYRNM,6,4),' ',Date.ACCTGPRDWOYNM) AS WEEK_LONG_NM,
RANK() OVER (PARTITION BY Date.ACCTGPRDMNTHID ORDER BY CALDT ASC) as DAY_NUM_IN_FISCAL_MONTH,
RANK() OVER (PARTITION BY Date.ACCTGPRDQTRID ORDER BY CALDT ASC) as DAY_NUM_IN_FISCAL_QUARTER,
RANK() OVER (PARTITION BY Date.ACCTGPRDYRID ORDER BY CALDT ASC) as DAY_NUM_IN_FISCAL_YEAR
FROM "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_DATE" as Date
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_QUARTER" as Quarter on Quarter.ACCTGPRDQTRID=Date.ACCTGPRDQTRID
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_WEEK" as Week on Week.ACCTGPRDWKID=Date.ACCTGPRDWKID
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_MONTH" as Month on Month.ACCTGPRDMNTHID=Date.ACCTGPRDMNTHID
ORDER BY CALDT DESC
),
Date_Table as 
(
Select
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
MAX(DAY_NUM_IN_FISCAL_MONTH) OVER (PARTITION BY ACCTGPRDMNTHID) as MAX_FISCAL_MONTH_DAY_NUM,
MAX(DAY_NUM_IN_FISCAL_QUARTER) OVER (PARTITION BY ACCTGPRDQTRID) as MAX_FISCAL_QUARTER_DAY_NUM,
MAX(DAY_NUM_IN_FISCAL_YEAR) OVER (PARTITION BY ACCTGPRDYRID) as MAX_FISCAL_YEAR_DAY_NUM
from Date_Table_Calc
),
--Made couple of necessary changes required for Demand data before joining the Product, Location and Date attributes to each record as the current state of Demand table only consists of R0700 Location Id's:
--BRAND_LOCATION: Built a Case statement to ensure that Walmart, Target and Ebay are tracked correctly with their respective BRAND_LOCATION values
--LOCATION_PK: Built a Case statement to ensure that Walmart, Target and Ebay are tracked correctly with their respective LOCATION_PK values
--DISCOUNT_AMOUNT_USD: Used a COALESCE function on DISCOUNT_AMOUNT_USD as the original data has NULL values present that error upon creating any calculations on top of it, hence the fix to replace NULL with 0
--MAO data will be read from the New Target table created in Snowflake called FACT_DEMAND_TARGET_TABLE
UPDATED_DEMAND as
(
Select 
(CASE WHEN MAO.ORDER_ID LIKE '%RBWMT%' THEN 'R6232'
      WHEN MAO.ORDER_ID LIKE '%RBTGT%' THEN 'R6233'
      WHEN MAO.ORDER_ID LIKE '%RBEBA%' THEN 'R0603'
      WHEN MAO.ORDER_ID LIKE '%RBEC%' THEN MAO.BRAND_LOCATION
      WHEN MAO.ORDER_ID LIKE '%RBCC%' THEN MAO.BRAND_LOCATION
      ELSE NULL
END) as BRAND_LOCATION,
(CASE WHEN MAO.ORDER_ID LIKE '%RBWMT%' THEN '013404a06dffa58412639be3c2f0e093'
      WHEN MAO.ORDER_ID LIKE '%RBTGT%' THEN 'b89e7dd25eec51a871105981d881457a'
      WHEN MAO.ORDER_ID LIKE '%RBEBA%' THEN 'cca1bbca0d17802a66186c03c6b3442d'
      WHEN MAO.ORDER_ID LIKE '%RBEC%' THEN MAO.LOCATION_PK
      WHEN MAO.ORDER_ID LIKE '%RBCC%' THEN MAO.LOCATION_PK
      ELSE NULL
END) as LOCATION_PK,
MAO.DEMAND_DATE_LOCAL,
MAO.ITEM_PK,
MAO.ORDER_TYPE,
MAO.ORDER_PK,
MAO.ORDER_ID,
--MAO.MIN_FULFILLMENT_STATUS_ID,
MAO.MAX_FULFILLMENT_STATUS_ID,
MAO.IS_RETURN,
MAO.IS_CANCELLED,
MAO.IS_GIFT_CARD,
MAO.DEMAND_UNITS,
MAO.DEMAND_VENDOR_COST_USD,
MAO.DEMAND_AVERAGE_COST_USD,
MAO.DEMAND_DOLLARS_USD,
COALESCE(MAO.DISCOUNT_AMOUNT_USD, 0) AS DISCOUNT_AMOUNT_USD,
MAO.PRICE_TYPE,
MAO.SALES_CHANNEL,
MAO.FULFILLMENT_ID,
MAO.SOURCE
from "SPARC_BASE"."ECOM_ANALYTICS"."FACT_DEMAND_TARGET_TABLE" as MAO
)
--Final Aggregation:
--MAX_FULFILLMENT_STATUS_ID: Converted MAX_FULFILLMENT_STATUS_ID from String to a Number
--OPEN_ORDER_ID: Created a Flag that identifies records which are currently in an OPEN state based on MAX_FULFILLMENT_STATUS_ID's
Select
BRAND_LOCATION,
LOCATION_PK,
DEMAND_DATE_LOCAL,
ITEM_PK,
ORDER_TYPE,
ORDER_PK,
ORDER_ID,
--MIN_FULFILLMENT_STATUS_ID,
MAX_FULFILLMENT_STATUS_ID,
CURRENT_STATUS,
(CASE WHEN MAX_FULFILLMENT_STATUS_ID < 7000 THEN ORDER_ID ELSE NULL END) as OPEN_ORDER_ID,
IS_RETURN,
IS_CANCELLED,
IS_GIFT_CARD,
sum(DEMAND_UNITS) as DEMAND_UNITS,
sum(DEMAND_VENDOR_COST_USD) as DEMAND_VENDOR_COST_USD,
sum(DEMAND_AVERAGE_COST_USD) as DEMAND_AVERAGE_COST_USD,
sum(DEMAND_UNITS*DEMAND_AVERAGE_COST_USD) as DEMAND_TOTAL_COST,
sum(DEMAND_DOLLARS_USD) as DEMAND_DOLLARS_USD,
sum(DISCOUNT_AMOUNT_USD) as DISCOUNT_AMOUNT_USD,
sum(NET_DEMAND_DOLLARS) as NET_DEMAND_DOLLARS,
REPORTABLE_ORDER_ID,
PRICE_TYPE, 
SALES_CHANNEL,
FULFILLMENT_ID,
SOURCE,
ARTICLE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
DIVISION_NAME,
STYLE_NAME,
MODEL_NUMBER,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE_DESC,
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
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
MAX_FISCAL_MONTH_DAY_NUM,
MAX_FISCAL_QUARTER_DAY_NUM,
MAX_FISCAL_YEAR_DAY_NUM
from(
--CURRENT_STATUS: Defined a description for every Status code in MAX_FULFILLMENT_STATUS_ID to determine the Current status of a record
--NET_DEMAND_DOLLARS: To calculate NET_DEMAND_DOLLARS, we need to ensure that all 3 flags (IsReturn,IsCancelled and IsGiftCard) are all 0 to then sum up DEMAND_DOLLARS_USD and DISCOUNT_AMOUNT_USD
--REPORTABLE_ORDER_ID: To calculate REPORTABLE_ORDER_ID, we need to ensure that all 3 flags (IsReturn,IsCancelled and IsGiftCard) are all 0 to then set the ORDER_ID value for distinct count purposes at the dashboard level
--PRICE_TYPE: Funneled down PRICE_TYPE to 3 categories (Markdown, FullPrice and Other), all Members are funneled down as a Markdown value
Select
MAO.BRAND_LOCATION,
MAO.LOCATION_PK,
MAO.DEMAND_DATE_LOCAL,
MAO.ITEM_PK,
MAO.ORDER_TYPE,
MAO.ORDER_PK,
MAO.ORDER_ID,
--MAO.MIN_FULFILLMENT_STATUS_ID,
TO_NUMBER(MAO.MAX_FULFILLMENT_STATUS_ID) as MAX_FULFILLMENT_STATUS_ID,
(CASE
    WHEN MAO.MAX_FULFILLMENT_STATUS_ID=MAO_STATUS.MAX_FULFILLMENT_STATUS_ID THEN MAO_STATUS.DESCRIPTION
    ELSE NULL
END) as CURRENT_STATUS,
MAO.IS_RETURN,
MAO.IS_CANCELLED,
MAO.IS_GIFT_CARD,
MAO.DEMAND_UNITS,
MAO.DEMAND_VENDOR_COST_USD,
MAO.DEMAND_AVERAGE_COST_USD,
MAO.DEMAND_DOLLARS_USD,
MAO.DISCOUNT_AMOUNT_USD,
(CASE
    WHEN (MAO.IS_RETURN=0 AND MAO.IS_CANCELLED=0 AND MAO.IS_GIFT_CARD=0) THEN (MAO.DEMAND_DOLLARS_USD + MAO.DISCOUNT_AMOUNT_USD)
    ELSE 0
END) as NET_DEMAND_DOLLARS,
(CASE 
    WHEN (MAO.IS_RETURN=0 AND MAO.IS_CANCELLED=0 AND MAO.IS_GIFT_CARD=0) THEN MAO.ORDER_ID
    ELSE NULL
END) as REPORTABLE_ORDER_ID,
(CASE
    WHEN MAO.PRICE_TYPE='Member' THEN 'Markdown'
    WHEN MAO.PRICE_TYPE='Markdown' THEN MAO.PRICE_TYPE
    WHEN MAO.PRICE_TYPE='Full Price' THEN MAO.PRICE_TYPE
    ELSE 'Other'
END) AS PRICE_TYPE,
MAO.SALES_CHANNEL,
MAO.FULFILLMENT_ID,
MAO.SOURCE,
Product.ARTICLE,
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
Product.BUSINESS_SEGMENT_DESC,
Product.KEY_CATEGORY_DESC,
Product.FRANCHISE_DESC,
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
Date_Table.CALDT,
Date_Table.ACCTGPRDQTRNUM,
Date_Table.ACCTGPRDQTRLNGNM,
Date_Table.ACCTGPRDYRNM,
Date_Table.ACCTGPRDYR,
Date_Table.ACCTGPRDQTRNM,
Date_Table.ACCTGPRDMNTHNM,
Date_Table.ACCTGPRDWOYNM,
Date_Table.ACCTGPRDWOYNM_NUMBER,
Date_Table.SEASNNM,
Date_Table.ACCTGPRDYRWKNUM,
Date_Table.ACCTGPRDMNTHNUM,
Date_Table.MONTH_LONG_NM,
Date_Table.WEEK_LONG_NM,
Date_Table.DAY_NUM_IN_FISCAL_MONTH,
Date_Table.DAY_NUM_IN_FISCAL_QUARTER,
Date_Table.DAY_NUM_IN_FISCAL_YEAR,
Date_Table.MAX_FISCAL_MONTH_DAY_NUM,
Date_Table.MAX_FISCAL_QUARTER_DAY_NUM,
Date_Table.MAX_FISCAL_YEAR_DAY_NUM
from UPDATED_DEMAND as MAO
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_PRODUCT" as Product on Product.ITEM_PK=MAO.ITEM_PK
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_LOCATION" as Location on Location.LOCATION_PK=MAO.LOCATION_PK
LEFT JOIN Date_Table as Date_Table on Date_Table.CALDT=MAO.DEMAND_DATE_LOCAL
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."MAO_FULFILLMENT_STATUS_ID" as MAO_STATUS on MAO.MAX_FULFILLMENT_STATUS_ID=MAO_STATUS.MAX_FULFILLMENT_STATUS_ID
--Main WHERE clause to limit data between Go-Live date and the day prior to current date(yesterday) after every truncate refresh
WHERE MAO.DEMAND_DATE_LOCAL >= '2023-05-08'
AND MAO.DEMAND_DATE_LOCAL < CURRENT_DATE
)
GROUP BY
BRAND_LOCATION,
LOCATION_PK,
DEMAND_DATE_LOCAL,
ITEM_PK,
ORDER_TYPE,
ORDER_PK,
ORDER_ID,
--MIN_FULFILLMENT_STATUS_ID,
MAX_FULFILLMENT_STATUS_ID,
CURRENT_STATUS,
OPEN_ORDER_ID,
IS_RETURN,
IS_CANCELLED,
IS_GIFT_CARD,
REPORTABLE_ORDER_ID,
PRICE_TYPE, 
SALES_CHANNEL,
FULFILLMENT_ID,
SOURCE,
ARTICLE,
ITEM_SEASON_NAME,
PRODUCT_GROUP,
ARTICLE_CREATION_SEASON,
ARTICLE_ACTIVE_SEASON,
DIVISION_NAME,
STYLE_NAME,
MODEL_NUMBER,
VENDOR_ID,
FORECAST_IND,
ORIGINAL_RETAIL,
BUSINESS_SEGMENT_DESC,
KEY_CATEGORY_DESC,
FRANCHISE_DESC,
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
SEASNNM,
ACCTGPRDYRWKNUM,
ACCTGPRDMNTHNUM,
MONTH_LONG_NM,
WEEK_LONG_NM,
DAY_NUM_IN_FISCAL_MONTH,
DAY_NUM_IN_FISCAL_QUARTER,
DAY_NUM_IN_FISCAL_YEAR,
MAX_FISCAL_MONTH_DAY_NUM,
MAX_FISCAL_QUARTER_DAY_NUM,
MAX_FISCAL_YEAR_DAY_NUM