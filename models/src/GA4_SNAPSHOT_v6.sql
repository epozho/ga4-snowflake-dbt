{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}

Select * from (
Select 
GA4.*,
Traffic.TRAFFIC_SOURCE_NAME as LastTouch_Traffic_Campaign,
Traffic.TRAFFIC_SOURCE_MEDIUM as LastTouch_Traffic_Medium,
Traffic.TRAFFIC_SOURCE_SOURCE as LastTouch_Traffic_Source,
Product.DIVISION_NAME,
Product.KEY_CATEGORY_DESC,
Product.BUSINESS_SEGMENT_DESC,
Date.CALDT as CALDT,
Quarter.ACCTGPRDQTRNUM as ACCTGPRDQTRNUM,
Quarter.ACCTGPRDQTRLNGNM as ACCTGPRDQTRLNGNM,
Date.ACCTGPRDYRNM,
SUBSTR(Date.ACCTGPRDYRNM,6,4) as ACCTGPRDYR,
Quarter.ACCTGPRDQTRNM as ACCTGPRDQTRNM,
Date.ACCTGPRDMNTHNM,
Date.ACCTGPRDWOYNM,
SUBSTR(Date.ACCTGPRDWOYNM,6,2) as ACCTGPRDWOYNM_NUMBER,
Date.SEASNNM as SEASNNM,
Week.ACCTGPRDYRWKNUM as ACCTGPRDYRWKNUM,
CONCAT(TO_VARCHAR(EXTRACT(YEAR FROM Month.ACCTGPRDMNTHENDDT)),TO_VARCHAR(CASE WHEN LENGTH(Month.ACCTGPRDMNTHNUM)=1 THEN CONCAT('0',TO_VARCHAR(Month.ACCTGPRDMNTHNUM)) ELSE TO_VARCHAR(Month.ACCTGPRDMNTHNUM) END)) as ACCTGPRDMNTHNUM 
(
Select 
max(CHECKOUT_STAGE_ORDER) OVER(PARTITION BY USER_SESSION_KEY) as MAX_CHECKOUT_STAGE_ORDER,
(CASE
    WHEN MAX_CHECKOUT_STAGE_ORDER=1 THEN 'add_to_cart'
    WHEN MAX_CHECKOUT_STAGE_ORDER=2 THEN 'view_cart' 
    WHEN MAX_CHECKOUT_STAGE_ORDER=3 THEN 'begin_checkout' 
    WHEN MAX_CHECKOUT_STAGE_ORDER=4 THEN 'add_shipping_info'
    WHEN MAX_CHECKOUT_STAGE_ORDER=5 THEN 'add_payment_info' 
    WHEN MAX_CHECKOUT_STAGE_ORDER=6 THEN 'review_order' 
    WHEN MAX_CHECKOUT_STAGE_ORDER=7 THEN 'purchase'
    ELSE NULL
END) as MAX_CHECKOUT_STAGE,
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
STYLE,
FRANCHISE,
--ITEMS_ITEM_VARIANT,
--ITEMS_ITEM_CATEGORY,
GENDER,
CORPORATE_MARKETING_LINE,
PRODUCT_TYPE,
CATEGORY_MARKETING_LINE,
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
ITEMS_ITEM_NAME as STYLE,
ITEMS_ITEM_BRAND as FRANCHISE,
--ITEMS_ITEM_VARIANT,
--ITEMS_ITEM_CATEGORY,
ITEMS_ITEM_CATEGORY2 as GENDER,
ITEMS_ITEM_CATEGORY3 as CORPORATE_MARKETING_LINE,
ITEMS_ITEM_CATEGORY4 as PRODUCT_TYPE,
ITEMS_ITEM_CATEGORY5 as CATEGORY_MARKETING_LINE,
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
	ITEMS_ITEM_NAME,
	ITEMS_ITEM_BRAND,
	--ITEMS_ITEM_VARIANT,
	--ITEMS_ITEM_CATEGORY,
	ITEMS_ITEM_CATEGORY2,
	ITEMS_ITEM_CATEGORY3,
	ITEMS_ITEM_CATEGORY4,
	ITEMS_ITEM_CATEGORY5,
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
	    ITEMS_ITEM_NAME ,
	    ITEMS_ITEM_BRAND ,
	    --ITEMS_ITEM_VARIANT ,
        --ITEMS_ITEM_CATEGORY ,
        ITEMS_ITEM_CATEGORY2 ,
        ITEMS_ITEM_CATEGORY3 ,
        ITEMS_ITEM_CATEGORY4 ,
        ITEMS_ITEM_CATEGORY5 ,
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
                 items_item_name , 
                 items_item_brand , 
                 --items_item_variant , 
                 --items_item_category , 
                 items_item_category2 , 
                 items_item_category3 , 
                 items_item_category4 , 
                 items_item_category5 ,
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
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_DATE" as Date on Date.CALDT=GA4.EVENT_DATE
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_QUARTER" as Quarter on Quarter.ACCTGPRDQTRID=Date.ACCTGPRDQTRID
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_WEEK" as Week on Week.ACCTGPRDWKID=Date.ACCTGPRDWKID
LEFT JOIN "SPARC_BASE"."ECOM_ANALYTICS"."DIM_TIME_CALENDAR_MONTH" as Month on Month.ACCTGPRDMNTHID=Date.ACCTGPRDMNTHID
ORDER BY GA4.USER_SESSION_KEY, GA4.EVENT_DATE DESC
)

