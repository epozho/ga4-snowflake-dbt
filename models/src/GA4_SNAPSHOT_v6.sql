{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}


With 
Date_Table as
(
  Select
Date.CALDT AS CALDT,
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
)
SELECT
MAX_CHECKOUT_STAGE_ORDER,
MAX_CHECKOUT_STAGE,
CHECKOUT_STAGE_ORDER,
EVENT_DATE,
EVENT_TIMESTAMP,
EVENT_NAME_ORIGINAL,
USER_SESSION_KEY,
EVENT_BUNDLE_SEQUENCE_ID,
USER_PSEUDO_ID,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
UNITS_ORDERED,
ITEMS_ITEM_ID,
--UPPER(STYLE) as STYLE_NAME,
FRANCHISE,
GENDER,
ITEMS_PRICE_IN_USD,
ITEMS_ITEM_REVENUE_IN_USD,
EP_PAGE_TYPE,
EP_PAGE_OWNER,
EP_ENTRANCES,
EP_GA_SESSION_ID,
EP_CHECKOUT_ORDERDISCOUNTVALUE,
EVENT_ADD_TO_CART,
EVENT_LOGIN,
EVENT_VIEW_ITEM,
EVENT_BEGIN_CHECKOUT,
EVENT_SELECT_ITEM,
EVENT_FIRST_VISIT,
EVENT_SESSION_START,
EVENT_PURCHASE,
EVENT_SEARCH,
EVENT_REMOVE_FROM_CART,
EVENT_USER_ENGAGEMENT,
EVENT_VIEW_ITEM_LIST,
EVENT_PAGE_VIEW,
EVENT_ADD_PAYMENT_INFO,
EVENT_ADD_TO_WISHLIST,
EVENT_SIGN_UP_NEWSLETTER,
EVENT_SIGN_UP,
EVENT_VIEW_CART,
EVENT_ADD_SHIPPING_INFO,
EVENT_NAVIGATION,
EVENT_SELECT_PROMOTION,
EVENT_VIEW_SEARCH_RESULTS,
EVENT_FETCH_USER_DATA,
EVENT_REVIEW_ORDER,
EVENT_ERROR_404,
EVENT_SORT_APPLIED,
EVENT_OUT_OF_STOCK_SIGNUP,
EVENT_FILTER_ADDED,
EVENT_NO_SEARCH_RESULTS,
EVENT_FORM_ERROR,
LASTTOUCH_TRAFFIC_CAMPAIGN,
LASTTOUCH_TRAFFIC_MEDIUM,
LASTTOUCH_TRAFFIC_SOURCE,
(CASE
	WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%affiliate%' THEN 'Affiliates'
	WHEN (LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%display%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%banner%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%expandable%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%interstitial%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%cpm%') THEN 'Display'
	WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM)='organic' THEN 'Organic Search'
	WHEN REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN), '^(.*(([^a-df-z]|^)shop|shopping).*)$') OR LOWER(LASTTOUCH_TRAFFIC_SOURCE)='shopping free listings' THEN 'Organic Shopping'
    WHEN (LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%social%' AND LOWER(LASTTOUCH_TRAFFIC_MEDIUM) NOT LIKE '%paid%') OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%social-network%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%social-media%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%sm%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%social network%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%social media%' THEN 'Organic Social'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%#_brand#_%' ESCAPE '#') THEN 'Paid Search - Branded'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%#_nb#_%' ESCAPE '#') THEN 'Paid Search - NB'
    WHEN (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_MEDIUM), '^(.*cp.*|ppc|retargeting|paid.*)$')) AND (REGEXP_LIKE(LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN), '^(.*(([^a-df-z]|^)shop|shopping).*)$')) THEN 'Paid Shopping'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%paidsocial%' THEN 'Paid Social'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) LIKE '%sfmc_promo%' THEN 'Promotional Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%referral%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%app%' OR LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%link%' THEN 'Referral'
    WHEN LOWER(LASTTOUCH_TRAFFIC_MEDIUM) LIKE '%sms%' THEN 'SMS Marketing'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) LIKE '%sfmc_transactional%' THEN 'Transactional Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) LIKE '%sfmc_trigger%' THEN 'Triggered Email'
    WHEN LOWER(LASTTOUCH_TRAFFIC_SOURCE) LIKE '%(direct)%' THEN 'Web Direct'
    ELSE NULL
END) as MEDIA_CHANNEL,
(CASE 
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%midfunnel%' THEN 'Mid-Funnel'
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%ecom%' THEN 'Performance Marketing'
    WHEN  LOWER(LASTTOUCH_TRAFFIC_CAMPAIGN) LIKE '%upperfunnel%' THEN 'Brand'
    WHEN  MEDIA_CHANNEL='Transactional Email' OR MEDIA_CHANNEL='Triggered Email' OR MEDIA_CHANNEL='Promotional Email' THEN 'Email'
    WHEN  MEDIA_CHANNEL='SMS Marketing' THEN 'SMS'
    WHEN  MEDIA_CHANNEL='Affiliates' THEN 'Affiliates'
    ELSE 'UNDEFINED'
END) AS FUNNEL,
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
DAY_NUM_IN_FISCAL_YEAR
FROM (
    SELECT 
        GA4.*,
        Traffic.TRAFFIC_SOURCE_NAME AS LastTouch_Traffic_Campaign,
        Traffic.TRAFFIC_SOURCE_MEDIUM AS LastTouch_Traffic_Medium,
        Traffic.TRAFFIC_SOURCE_SOURCE AS LastTouch_Traffic_Source,
        Product.DIVISION_NAME,
        Product.KEY_CATEGORY_DESC,
        Product.BUSINESS_SEGMENT_DESC,
        Date_Table.CALDT AS CALDT,
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
        Date_Table.DAY_NUM_IN_FISCAL_YEAR
    FROM (
        SELECT 
            MAX(CHECKOUT_STAGE_ORDER) OVER (PARTITION BY USER_SESSION_KEY) AS MAX_CHECKOUT_STAGE_ORDER,
            (CASE 
                WHEN MAX_CHECKOUT_STAGE_ORDER=1 THEN 'add_to_cart'
                WHEN MAX_CHECKOUT_STAGE_ORDER=2 THEN 'view_cart' 
                WHEN MAX_CHECKOUT_STAGE_ORDER=3 THEN 'begin_checkout' 
                WHEN MAX_CHECKOUT_STAGE_ORDER=4 THEN 'add_shipping_info'
                WHEN MAX_CHECKOUT_STAGE_ORDER=5 THEN 'add_payment_info' 
                WHEN MAX_CHECKOUT_STAGE_ORDER=6 THEN 'review_order' 
                WHEN MAX_CHECKOUT_STAGE_ORDER=7 THEN 'purchase'
                ELSE NULL
            END) AS MAX_CHECKOUT_STAGE,
CHECKOUT_STAGE_ORDER,
EVENT_DATE,
EVENT_TIMESTAMP ,
EVENT_NAME_ORIGINAL,
USER_SESSION_KEY,
EVENT_BUNDLE_SEQUENCE_ID,
USER_PSEUDO_ID,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
UNITS_ORDERED,
--TRAFFIC_SOURCE_NAME as CAMPAIGN,
--TRAFFIC_SOURCE_MEDIUM as MEDIUM,
--TRAFFIC_SOURCE_SOURCE as SOURCE,
ITEMS_ITEM_ID,
--STYLE,
FRANCHISE,
--ITEMS_ITEM_VARIANT,
--ITEMS_ITEM_CATEGORY,
GENDER,
--CORPORATE_MARKETING_LINE,
--PRODUCT_TYPE,
--CATEGORY_MARKETING_LINE,
ITEMS_PRICE_IN_USD ,
--ITEMS_QUANTITY ,
ITEMS_ITEM_REVENUE_IN_USD,
EP_PAGE_TYPE,
--EP_ENGAGED_SESSION_EVENT,
EP_PAGE_OWNER,
--EP_MEDIUM,
--EP_FIREBASE_CONVERSION,
--EP_SESSION_ENGAGED,
EP_ENTRANCES,
--EP_TRANSACTION_ID,
--EP_CAMPAIGN,
EP_GA_SESSION_ID,
EP_CHECKOUT_ORDERDISCOUNTVALUE,
--EP_SOURCE,
--EP_GA_SESSION_NUMBER,
Event_add_to_cart,
Event_login,
Event_view_item,
Event_begin_checkout,
Event_select_item,
Event_first_visit,
Event_session_start,
Event_purchase,
Event_search,
Event_remove_from_cart,
Event_user_engagement,
Event_view_item_list,
Event_page_view,
Event_add_payment_info,
Event_add_to_wishlist,
Event_sign_up_newsletter,
Event_sign_up,
Event_view_cart,
Event_add_shipping_info,
Event_navigation,
Event_select_promotion,
Event_view_search_results,
Event_fetch_user_data,
Event_review_order,
Event_error_404,
Event_sort_applied,
Event_out_of_stock_signup,
Event_filter_added,
Event_no_search_results,
Event_form_error
from (
Select
(CASE
    WHEN EVENT_NAME_ORIGINAL='add_to_cart' THEN 1
    WHEN EVENT_NAME_ORIGINAL='view_cart' THEN 2
    WHEN EVENT_NAME_ORIGINAL='begin_checkout' THEN 3
    WHEN EVENT_NAME_ORIGINAL='add_shipping_info' THEN 4
    WHEN EVENT_NAME_ORIGINAL='add_payment_info' THEN 5
    WHEN EVENT_NAME_ORIGINAL='review_order' THEN 6
    WHEN EVENT_NAME_ORIGINAL='purchase' THEN 7
    ELSE NULL
END) as CHECKOUT_STAGE_ORDER,
EVENT_DATE,
EVENT_TIMESTAMP ,
EVENT_NAME_ORIGINAL,
USER_SESSION_KEY,
EVENT_BUNDLE_SEQUENCE_ID,
USER_PSEUDO_ID,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
(CASE WHEN EVENT_NAME_ORIGINAL='purchase' THEN ITEMS_QUANTITY ELSE NULL END) AS UNITS_ORDERED,
--TRAFFIC_SOURCE_NAME as CAMPAIGN,
--TRAFFIC_SOURCE_MEDIUM as MEDIUM,
--TRAFFIC_SOURCE_SOURCE as SOURCE,
ITEMS_ITEM_ID,
--ITEMS_ITEM_NAME as STYLE,
ITEMS_ITEM_BRAND as FRANCHISE,
--ITEMS_ITEM_VARIANT,
--ITEMS_ITEM_CATEGORY,
ITEMS_ITEM_CATEGORY2 as GENDER,
--ITEMS_ITEM_CATEGORY3 as CORPORATE_MARKETING_LINE,
--ITEMS_ITEM_CATEGORY4 as PRODUCT_TYPE,
--ITEMS_ITEM_CATEGORY5 as CATEGORY_MARKETING_LINE,
ITEMS_PRICE_IN_USD ,
ITEMS_QUANTITY ,
ITEMS_ITEM_REVENUE_IN_USD,
EP_PAGE_TYPE,
--EP_ENGAGED_SESSION_EVENT,
EP_PAGE_OWNER,
--EP_MEDIUM,
--EP_FIREBASE_CONVERSION,
--EP_SESSION_ENGAGED,
EP_ENTRANCES,
--EP_TRANSACTION_ID,
--EP_CAMPAIGN,
EP_GA_SESSION_ID,
EP_CHECKOUT_ORDERDISCOUNTVALUE,
--EP_SOURCE,
--EP_GA_SESSION_NUMBER,
TO_NUMBER("'add_to_cart'") as Event_add_to_cart,
TO_NUMBER("'login'") as Event_login,
TO_NUMBER("'view_item'") as Event_view_item,
TO_NUMBER("'begin_checkout'") as Event_begin_checkout,
TO_NUMBER("'select_item'") as Event_select_item,
TO_NUMBER("'first_visit'") as Event_first_visit,
TO_NUMBER("'session_start'") as Event_session_start,
TO_NUMBER("'purchase'") as Event_purchase,
TO_NUMBER("'search'") as Event_search,
TO_NUMBER("'remove_from_cart'") as Event_remove_from_cart,
TO_NUMBER("'user_engagement'") as Event_user_engagement,
TO_NUMBER("'view_item_list'") as Event_view_item_list,
TO_NUMBER("'page_view'") as Event_page_view,
TO_NUMBER("'add_payment_info'") as Event_add_payment_info,
TO_NUMBER("'add_to_wishlist'") as Event_add_to_wishlist,
TO_NUMBER("'sign_up_newsletter'") as Event_sign_up_newsletter,
TO_NUMBER("'sign_up'") as Event_sign_up,
TO_NUMBER("'view_cart'") as Event_view_cart,
TO_NUMBER("'add_shipping_info'") as Event_add_shipping_info,
TO_NUMBER("'navigation'") as Event_navigation,
TO_NUMBER("'select_promotion'") as Event_select_promotion,
TO_NUMBER("'view_search_results'") as Event_view_search_results,
TO_NUMBER("'fetch_user_data'") as Event_fetch_user_data,
TO_NUMBER("'review_order'") as Event_review_order,
TO_NUMBER("'error_404'") as Event_error_404,
TO_NUMBER("'sort_applied'") as Event_sort_applied,
TO_NUMBER("'out_of_stock_signup'") as Event_out_of_stock_signup,
TO_NUMBER("'filter_added'") as Event_filter_added,
TO_NUMBER("'no_search_results'") as Event_no_search_results,
TO_NUMBER("'form_error'") as Event_form_error
from
(Select
    EVENT_DATE,
	EVENT_TIMESTAMP,
    EVENT_NAME as EVENT_NAME_ORIGINAL,
	EVENT_NAME,
    1 as EVENT_NAME_FLAG,
    CONCAT(USER_PSEUDO_ID,'|',EP_GA_SESSION_ID) as USER_SESSION_KEY,
    EVENT_BUNDLE_SEQUENCE_ID,
	USER_PSEUDO_ID,
    ECOMMERCE_TRANSACTION_ID,
	DEVICE_CATEGORY,
	--TRAFFIC_SOURCE_NAME,
	--TRAFFIC_SOURCE_MEDIUM,
	--TRAFFIC_SOURCE_SOURCE,
	ITEMS_ITEM_ID,
	--ITEMS_ITEM_NAME,
	ITEMS_ITEM_BRAND,
	--ITEMS_ITEM_VARIANT,
	--ITEMS_ITEM_CATEGORY,
	ITEMS_ITEM_CATEGORY2,
	--ITEMS_ITEM_CATEGORY3,
	--ITEMS_ITEM_CATEGORY4,
	--ITEMS_ITEM_CATEGORY5,
    ITEMS_PRICE_IN_USD ,
    ITEMS_QUANTITY ,
    ITEMS_ITEM_REVENUE_IN_USD,
	EP_PAGE_TYPE,
	--EP_ENGAGED_SESSION_EVENT,
	EP_PAGE_OWNER,
	--EP_MEDIUM,
	--EP_FIREBASE_CONVERSION,
	--EP_SESSION_ENGAGED,
	EP_ENTRANCES,
	--EP_TRANSACTION_ID,
	--EP_CAMPAIGN,
	EP_GA_SESSION_ID,
	EP_CHECKOUT_ORDERDISCOUNTVALUE
	--EP_SOURCE,
	--EP_GA_SESSION_NUMBER
from 
    (
        SELECT
	    EVENT_DATE ,
	    EVENT_TIMESTAMP,
	    EVENT_NAME ,
        EVENT_BUNDLE_SEQUENCE_ID,
	    USER_PSEUDO_ID ,
        ECOMMERCE_TRANSACTION_ID, 
	    DEVICE_CATEGORY ,
	    --TRAFFIC_SOURCE_NAME ,
	    --TRAFFIC_SOURCE_MEDIUM ,
	    --TRAFFIC_SOURCE_SOURCE ,
	    ITEMS_ITEM_ID ,
	    --ITEMS_ITEM_NAME ,
	    ITEMS_ITEM_BRAND ,
	    --ITEMS_ITEM_VARIANT ,
        --ITEMS_ITEM_CATEGORY ,
        ITEMS_ITEM_CATEGORY2 ,
        --ITEMS_ITEM_CATEGORY3 ,
        --ITEMS_ITEM_CATEGORY4 ,
        --ITEMS_ITEM_CATEGORY5 ,
        ITEMS_PRICE_IN_USD ,
        ITEMS_QUANTITY ,
        ITEMS_ITEM_REVENUE_IN_USD,
        TO_VARCHAR("'page_type'") as EP_page_type,
        --TO_NUMBER("'engaged_session_event'") as EP_engaged_session_event,
        TO_VARCHAR("'page_owner'") as EP_page_owner,
        --TO_VARCHAR("'medium'") as EP_medium,
        --TO_NUMBER("'firebase_conversion'") as EP_firebase_conversion,
        --TO_NUMBER("'session_engaged'") as EP_session_engaged,
        TO_NUMBER("'entrances'") as EP_entrances,
        --TO_VARCHAR("'transaction_id'") as EP_transaction_id,
        --TO_VARCHAR("'campaign'") as EP_campaign,
	    TO_VARCHAR("'ga_session_id'") as EP_ga_session_id,
	    TO_NUMBER("'checkout_orderdiscountvalue'",10,2) as EP_checkout_orderdiscountvalue
	    --TO_VARCHAR("'source'") as EP_source,
	    --TO_NUMBER("'ga_session_number'") as EP_ga_session_number
        FROM (
                SELECT
                TO_DATE(EVENT_DATE,'YYYYMMDD') as EVENT_DATE,
                TO_TIMESTAMP(SUBSTR(EVENT_TIMESTAMP,1,13)) as EVENT_TIMESTAMP,
                EVENT_NAME,
                EVENT_PARAMS_KEY,
                 (CASE
                     WHEN EVENT_PARAMS_VALUE_STRING_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_STRING_VALUE)
                     WHEN EVENT_PARAMS_VALUE_INT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_INT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_FLOAT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_FLOAT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_DOUBLE_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_DOUBLE_VALUE)
                  ELSE NULL
                 END) AS EVENT_PARAMS_VALUE,
                 EVENT_BUNDLE_SEQUENCE_ID,
                 USER_PSEUDO_ID,
                 ecommerce_transaction_id,
                 device_category ,
                 --traffic_source_name , 
                 --traffic_source_medium , 
                 --traffic_source_source, 
                 items_item_id , 
                 --items_item_name , 
                 items_item_brand , 
                 --items_item_variant , 
                 --items_item_category , 
                 items_item_category2 , 
                 --items_item_category3 , 
                 --items_item_category4 , 
                 --items_item_category5 ,
                 ITEMS_PRICE_IN_USD ,
                 ITEMS_QUANTITY ,
                 ITEMS_ITEM_REVENUE_IN_USD
          FROM SPARC_RAW.RBOK_GA.GA4_EVENTS_RAW
        ) AS data_to_pivot
        PIVOT (
          MAX(EVENT_PARAMS_VALUE) FOR EVENT_PARAMS_KEY IN ('page_type',/*'engaged_session_event',*/'page_owner',/*'medium','firebase_conversion','session_engaged',*/'entrances',/*'transaction_id','campaign',*/'ga_session_id','checkout_orderdiscountvalue'/*,'source','ga_session_number'*/)
        ) AS pivoted_data
    )
) as data_to_pivot_1
PIVOT( MAX(EVENT_NAME_FLAG) FOR EVENT_NAME IN ('view_item','navigation','session_start','purchase','select_promotion','add_to_cart','login','begin_checkout','user_engagement','sign_up','view_search_results','remove_from_cart','fetch_user_data','review_order','error_404','sort_applied','first_visit','select_item','view_item_list','page_view','add_payment_info','add_to_wishlist','sign_up_newsletter','out_of_stock_signup','search','filter_added','no_search_results','add_shipping_info','view_cart','form_error')
) AS pivoted_data
)
)
as GA4 
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."LAST_TOUCH_TRAFFIC" as Traffic ON Traffic.USER_SESSION_KEY=GA4.USER_SESSION_KEY --143,733,650 rows
LEFT JOIN 
(
Select distinct 
Product.Article,
Product.DIVISION_NAME,
Product.KEY_CATEGORY_DESC,
Product.BUSINESS_SEGMENT_DESC
from "SPARC_BASE"."ECOM_ANALYTICS"."DIM_PRODUCT" as Product
WHERE KEY_CATEGORY_DESC is not null AND BUSINESS_SEGMENT_DESC is not null AND KEY_CATEGORY_DESC!='' AND BUSINESS_SEGMENT_DESC!=''
)
as Product ON Product.ARTICLE=GA4.ITEMS_ITEM_ID 
LEFT JOIN Date_Table as Date_Table on Date_Table.CALDT=GA4.EVENT_DATE
ORDER BY GA4.USER_SESSION_KEY, GA4.EVENT_DATE DESC
)

