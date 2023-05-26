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
--LAST_TOUCH_TRAFFIC is the last traffic attribute that exist for every event across Traffic Campaign, Traffic Medium and Traffic Source
--This is done by ranking the EVENT_TIMESTAMP by USER_SESSION_KEY and selecting the top Rank that contains the last touch traffic
--The below script is divided into 2 groups: 
--One is to ensure that any event that consists of purchase must have its corresponding values for last touch traffic to be populated by the latest Event timestamp of that purchase event only, this ignores any other events thats could be fired later
--Two is to ensure that any event that does not consist of purchase must have its corresponding values for last touch traffic to be populated by the latest Event timestamp
LAST_TOUCH_TRAFFIC AS
(
(Select USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL,TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE
from (
Select distinct row_number() OVER(PARTITION BY USER_SESSION_KEY ORDER BY EVENT_TIMESTAMP DESC) AS Rank,USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL,TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE --EP_MEDIUM, EP_CAMPAIGN, EP_SOURCE
from "SPARC_BASE"."ECOM_ANALYTICS"."GA4_FLATTENED"
WHERE USER_SESSION_KEY IN
(
    Select distinct USER_SESSION_KEY from "SPARC_BASE"."ECOM_ANALYTICS"."GA4_FLATTENED"
    WHERE EVENT_NAME_ORIGINAL='purchase'
    AND (TRAFFIC_SOURCE_NAME is not null
    OR TRAFFIC_SOURCE_MEDIUM is not null
    OR TRAFFIC_SOURCE_SOURCE is not null)
)
AND EVENT_NAME_ORIGINAL='purchase'
AND (TRAFFIC_SOURCE_NAME is not null
    OR TRAFFIC_SOURCE_MEDIUM is not null
    OR TRAFFIC_SOURCE_SOURCE is not null)
GROUP BY USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL,TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE 
ORDER BY USER_SESSION_KEY, EVENT_TIMESTAMP DESC
)
WHERE RANK=1)

UNION ALL

(Select USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL,TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE 
from (
Select distinct row_number() OVER(PARTITION BY USER_SESSION_KEY ORDER BY EVENT_TIMESTAMP DESC) AS Rank,USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL, TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE --EP_MEDIUM, EP_CAMPAIGN, EP_SOURCE
from "SPARC_BASE"."ECOM_ANALYTICS"."GA4_FLATTENED"
WHERE USER_SESSION_KEY NOT IN
(
    Select distinct USER_SESSION_KEY from "SPARC_BASE"."ECOM_ANALYTICS"."GA4_FLATTENED"
    WHERE EVENT_NAME_ORIGINAL='purchase'
    AND (TRAFFIC_SOURCE_NAME is not null
    OR TRAFFIC_SOURCE_MEDIUM is not null
    OR TRAFFIC_SOURCE_SOURCE is not null)
)
GROUP BY USER_SESSION_KEY, EVENT_TIMESTAMP,EVENT_NAME_ORIGINAL,TRAFFIC_SOURCE_NAME, TRAFFIC_SOURCE_MEDIUM, TRAFFIC_SOURCE_SOURCE 
ORDER BY USER_SESSION_KEY, EVENT_TIMESTAMP DESC
)
WHERE RANK=1)
),
--GA4_FINAL is responsible to read all columns that were flattened in GA4_FLATTENED and LEFT JOIN onto LastTouchTraffic, Products, Location and Date table attributes
--ENGAGED_GA_USER_SESSION_KEY: Created a flag that populated USER_SESSION_KEY for any event that has a session engaged, useful for count distinct count of USER_SESSION_KEY's with active sessions
--MEDIA_CHANNEL: Addtional column to identify MEDIA_CHANNEL based on rules provided through the GA4 documentation
--FUNNEL: A further aggregation of the MEDIA_CHANNEL's that are funneled into their respective funnel categories
GA4_FINAL AS
(
SELECT
GA4.MAX_CHECKOUT_STAGE_ORDER,
GA4.MAX_CHECKOUT_STAGE,
GA4.CHECKOUT_STAGE_ORDER,
GA4.EVENT_DATE,
GA4.EVENT_TIMESTAMP,
GA4.EVENT_NAME_ORIGINAL,
GA4.USER_SESSION_KEY,
GA4.EVENT_BUNDLE_SEQUENCE_ID,
GA4.USER_PSEUDO_ID,
GA4.ECOMMERCE_TRANSACTION_ID,
GA4.DEVICE_CATEGORY,
GA4.UNITS_ORDERED,
GA4.ITEMS_ITEM_ID,
GA4.FRANCHISE,
GA4.GENDER,
GA4.ITEMS_PRICE_IN_USD,
GA4.ITEMS_ITEM_REVENUE_IN_USD,
GA4.EP_PAGE_TYPE,
GA4.EP_PAGE_OWNER,
GA4.EP_ENTRANCES,
GA4.EP_GA_SESSION_ID,
GA4.EP_CHECKOUT_ORDERDISCOUNTVALUE,
GA4.EP_SESSION_ENGAGED,
(CASE WHEN GA4.EP_SESSION_ENGAGED=1 THEN GA4.USER_SESSION_KEY ELSE NULL END) AS ENGAGED_GA_USER_SESSION_KEY,
GA4.EVENT_ADD_TO_CART,
GA4.EVENT_LOGIN,
GA4.EVENT_VIEW_ITEM,
GA4.EVENT_BEGIN_CHECKOUT,
GA4.EVENT_SELECT_ITEM,
GA4.EVENT_FIRST_VISIT,
GA4.EVENT_SESSION_START,
GA4.EVENT_PURCHASE,
GA4.EVENT_SEARCH,
GA4.EVENT_REMOVE_FROM_CART,
GA4.EVENT_USER_ENGAGEMENT,
GA4.EVENT_VIEW_ITEM_LIST,
GA4.EVENT_PAGE_VIEW,
GA4.EVENT_ADD_PAYMENT_INFO,
GA4.EVENT_ADD_TO_WISHLIST,
GA4.EVENT_SIGN_UP_NEWSLETTER,
GA4.EVENT_SIGN_UP,
GA4.EVENT_VIEW_CART,
GA4.EVENT_ADD_SHIPPING_INFO,
GA4.EVENT_NAVIGATION,
GA4.EVENT_SELECT_PROMOTION,
GA4.EVENT_VIEW_SEARCH_RESULTS,
GA4.EVENT_FETCH_USER_DATA,
GA4.EVENT_REVIEW_ORDER,
GA4.EVENT_ERROR_404,
GA4.EVENT_SORT_APPLIED,
GA4.EVENT_OUT_OF_STOCK_SIGNUP,
GA4.EVENT_FILTER_ADDED,
GA4.EVENT_NO_SEARCH_RESULTS,
GA4.EVENT_FORM_ERROR,
Traffic.TRAFFIC_SOURCE_NAME AS LASTTOUCH_TRAFFIC_CAMPAIGN,
Traffic.TRAFFIC_SOURCE_MEDIUM AS LASTTOUCH_TRAFFIC_MEDIUM,
Traffic.TRAFFIC_SOURCE_SOURCE AS LASTTOUCH_TRAFFIC_SOURCE,
(CASE
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'affiliate') THEN 'Affiliates'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE ANY ('%display%','%banner%','%expandable%','%interstitial%','%cpm%') THEN 'Display'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='organic' THEN 'Organic Search'
    WHEN REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN), '^(.*(([^a-df-z]|^)shop|shopping).*)$') OR LOWER(LASTTOUCH_TRAFFIC_SOURCE)='shopping free listings' THEN 'Organic Shopping'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'social') AND LOWER(LASTTOUCH_TRAFFIC_MEDIUM) NOT LIKE '%paid%') OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE ANY ('%social-network%','%social-media%','%sm%','%social network%','%social media%') THEN 'Organic Social'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%#_brand#_%' ESCAPE '#') THEN 'Paid Search - Branded'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%#_nb#_%' ESCAPE '#') THEN 'Paid Search - NB'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN), '^(.*(([^a-df-z]|^)shop|shopping).*)$')) THEN 'Paid Shopping'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'paidsocial') THEN 'Paid Social'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'sfmc_promo') THEN 'Promotional Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE ANY ('%referral%','%app%','%link%') THEN 'Referral'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'sms') THEN 'SMS Marketing'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'sfmc_transactional') THEN 'Transactional Email'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'sfmc_trigger') THEN 'Triggered Email'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_MEDIUM),'(direct)') THEN 'Web Direct'
    ELSE NULL
END) as MEDIA_CHANNEL,
(CASE CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsocial')
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsearch') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'upperfunnel')) THEN 'Paid Search - Upperfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsearch') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'midfunnel')) THEN 'Paid Search - Midfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsearch') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'ecom')) THEN 'Paid Search - eCom'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidshopping') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'midfunnel')) THEN 'Paid Shopping - Midfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidshopping') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'ecom')) THEN 'Paid Shopping - eCom'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'display') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'upperfunnel')) THEN 'Display - Upperfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'display') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'midfunnel')) THEN 'Display - Midfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'display') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'ecom')) THEN 'Display - eCom'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsocial') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'upperfunnel')) THEN 'Paid Social - Upperfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsocial') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'midfunnel')) THEN 'Paid Social - Midfunnel'
    WHEN (CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'paidsocial') AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'ecom')) THEN 'Paid Social - eCom'
    WHEN LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) IN ('(referral)','(app)','(link)') THEN 'Referral'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='sms' THEN 'Partner - SMS'
    WHEN CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'partner') THEN 'Partner - Generic'
    WHEN (LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='organic' AND CONTAINS(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN),'shop')) THEN 'Organic Shopping'
    WHEN (LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='organic' OR LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN)='(organic)') THEN 'Organic Search'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') THEN 'Organic Social'
    WHEN LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN)='(direct)' THEN 'Direct'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='affiliate' THEN 'Affiliate'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) IN ('sfmc_promo','bluecore_promo') THEN 'Promotional Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) IN ('sfmc_transactional','narvar_transactional') THEN 'Transactional Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) IN ('sfmc_trigger','bluecore_trigger') THEN 'Triggered Email'
    ELSE 'Unassigned'
END) as RBK_MEDIA_CHANNEL,
(CASE 
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%midfunnel%' THEN 'Mid-Funnel'
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%ecom%' THEN 'Performance Marketing'
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%upperfunnel%' THEN 'Brand'
    WHEN  MEDIA_CHANNEL='Transactional Email' OR MEDIA_CHANNEL='Triggered Email' OR MEDIA_CHANNEL='Promotional Email' THEN 'Email'
    WHEN  MEDIA_CHANNEL='SMS Marketing' THEN 'SMS'
    WHEN  MEDIA_CHANNEL='Affiliates' THEN 'Affiliates'
    ELSE 'UNDEFINED'
END) AS FUNNEL,
(CASE
    WHEN CONTAINS(RBK_MEDIA_CHANNEL,'eCom') THEN 'eCom PM'
    WHEN RBK_MEDIA_CHANNEL IN ('Partner - SMS','Affiliate','Promotional Email','Transactional Email','Triggered Email') THEN 'eCom Other'
    WHEN CONTAINS(RBK_MEDIA_CHANNEL,'Midfunnel') THEN 'Midfunnel'
    WHEN CONTAINS(RBK_MEDIA_CHANNEL,'Upperfunnel') THEN 'Upperfunnel'
    ELSE 'Other'
END) as RBK_FUNNEL,
Product.DIVISION_NAME,
Product.KEY_CATEGORY_DESC,
Product.BUSINESS_SEGMENT_DESC,
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
FROM "SPARC_BASE"."ECOM_ANALYTICS"."GA4_FLATTENED" as GA4
LEFT JOIN LAST_TOUCH_TRAFFIC as Traffic ON Traffic.USER_SESSION_KEY=GA4.USER_SESSION_KEY
LEFT JOIN 
--The Left join on this product table is to ensure that we only read product records that always contain value both in KEY_CATEGORY_DESC and BUSINESS_SEGMENT_DESC
(
Select distinct 
Product.Article,
Product.DIVISION_NAME,
Product.KEY_CATEGORY_DESC,
Product.BUSINESS_SEGMENT_DESC
from "SPARC_BASE"."ECOM_ANALYTICS"."DIM_PRODUCT" as Product
WHERE KEY_CATEGORY_DESC is not null AND BUSINESS_SEGMENT_DESC is not null AND KEY_CATEGORY_DESC!='' AND BUSINESS_SEGMENT_DESC!=''
) as Product ON Product.ARTICLE=GA4.ITEMS_ITEM_ID 
LEFT JOIN Date_Table as Date_Table on Date_Table.CALDT=GA4.EVENT_DATE
ORDER BY GA4.USER_SESSION_KEY, GA4.EVENT_DATE DESC
),
--Final Aggregation of GA4 data:
--The below script is divided into 2 groups: 
--First group is to Aggregate all of the purchase events successfully and ensure their values are not being mixed with non-purchased events
--Second group is to Aggregae all of the non-purchase events that tend to not have any revenue present
GA4_AGGREGATED AS
(
(Select
EVENT_DATE,
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
sum(UNITS_ORDERED) as UNITS_ORDERED,
ITEMS_ITEM_ID as ARTICLE,
UPPER(FRANCHISE) as FRANCHISE,
GENDER,
sum(ITEMS_ITEM_REVENUE_IN_USD) as ITEMS_ITEM_REVENUE_IN_USD,
ENGAGED_GA_USER_SESSION_KEY,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
RBK_MEDIA_CHANNEL,
FUNNEL,
RBK_FUNNEL,
DIVISION_NAME,
KEY_CATEGORY_DESC,
BUSINESS_SEGMENT_DESC,
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
from GA4_FINAL
WHERE EVENT_NAME_ORIGINAL='purchase'
GROUP BY
EVENT_DATE,
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
ARTICLE,
FRANCHISE,
GENDER,
ENGAGED_GA_USER_SESSION_KEY,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
RBK_MEDIA_CHANNEL,
FUNNEL,
RBK_FUNNEL,
DIVISION_NAME,
KEY_CATEGORY_DESC,
BUSINESS_SEGMENT_DESC,
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
ORDER BY EVENT_DATE, USER_SESSION_KEY DESC
)

UNION ALL

(
Select
EVENT_DATE,
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
sum(UNITS_ORDERED) as UNITS_ORDERED,
ARTICLE,
UPPER(FRANCHISE) as FRANCHISE,
GENDER,
sum(ITEMS_ITEM_REVENUE_IN_USD) as ITEMS_ITEM_REVENUE_IN_USD,
ENGAGED_GA_USER_SESSION_KEY,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
RBK_MEDIA_CHANNEL,
FUNNEL,
RBK_FUNNEL,
DIVISION_NAME,
KEY_CATEGORY_DESC,
BUSINESS_SEGMENT_DESC,
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
Select
EVENT_DATE,
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
NULL as ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
NULL as UNITS_ORDERED,
NULL as ARTICLE,
NULL as FRANCHISE,
NULL as GENDER,
NULL as ITEMS_ITEM_REVENUE_IN_USD,
ENGAGED_GA_USER_SESSION_KEY,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
RBK_MEDIA_CHANNEL,
FUNNEL,
RBK_FUNNEL,
NULL as DIVISION_NAME,
NULL as KEY_CATEGORY_DESC,
NULL as BUSINESS_SEGMENT_DESC,
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
from GA4_FINAL
WHERE USER_SESSION_KEY NOT IN
(
SELECT distinct USER_SESSION_KEY from GA4_FINAL
WHERE EVENT_NAME_ORIGINAL='purchase'
)
)
GROUP BY
EVENT_DATE,
USER_SESSION_KEY,
MAX_CHECKOUT_STAGE,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
ARTICLE,
FRANCHISE,
GENDER,
ENGAGED_GA_USER_SESSION_KEY,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
MEDIA_CHANNEL,
RBK_MEDIA_CHANNEL,
FUNNEL,
RBK_FUNNEL,
DIVISION_NAME,
KEY_CATEGORY_DESC,
BUSINESS_SEGMENT_DESC,
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
ORDER BY EVENT_DATE, USER_SESSION_KEY DESC
)
)
Select * from GA4_AGGREGATED
