{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}


WITH 
CLEAN_GA4_EVENT_PARAMS_VALUE AS
(
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
	    		 TRAFFIC_SOURCE_NAME ,
	    		 TRAFFIC_SOURCE_MEDIUM ,
	    		 TRAFFIC_SOURCE_SOURCE ,
                 items_item_id , 
                 items_item_brand ,  
                 items_item_category2 ,
                 ITEMS_PRICE_IN_USD ,
                 ITEMS_QUANTITY ,
                 ITEMS_ITEM_REVENUE_IN_USD
          FROM SPARC_RAW.RBOK_GA.GA4_EVENTS_RAW
),
FIRST_GA4_FLATTEN AS
(
	SELECT
        EVENT_DATE ,
        EVENT_TIMESTAMP,
    	EVENT_NAME as EVENT_NAME_ORIGINAL,
        EVENT_NAME ,
    	1 as EVENT_NAME_FLAG,
        EVENT_BUNDLE_SEQUENCE_ID,
        USER_PSEUDO_ID ,
        ECOMMERCE_TRANSACTION_ID, 
        DEVICE_CATEGORY ,
	    TRAFFIC_SOURCE_NAME ,
	    TRAFFIC_SOURCE_MEDIUM ,
	    TRAFFIC_SOURCE_SOURCE ,
        ITEMS_ITEM_ID ,
        ITEMS_ITEM_BRAND ,
        ITEMS_ITEM_CATEGORY2 ,
        ITEMS_PRICE_IN_USD ,
        ITEMS_QUANTITY ,
        ITEMS_ITEM_REVENUE_IN_USD,
        TO_VARCHAR("'page_type'") as EP_PAGE_TYPE,
        TO_VARCHAR("'page_owner'") as EP_PAGE_OWNER,
        TO_NUMBER("'entrances'") as EP_ENTRANCES,
        TO_VARCHAR("'ga_session_id'") as EP_GA_SESSION_ID,
        CONCAT(USER_PSEUDO_ID,'|',EP_GA_SESSION_ID) as USER_SESSION_KEY,
        TO_NUMBER("'checkout_orderdiscountvalue'",10,2) as EP_CHECKOUT_ORDERDISCOUNTVALUE,
	    TO_NUMBER("'session_engaged'") as EP_SESSION_ENGAGED
        FROM CLEAN_GA4_EVENT_PARAMS_VALUE AS data_to_pivot
        PIVOT (
          MAX(EVENT_PARAMS_VALUE) FOR EVENT_PARAMS_KEY IN ('page_type','page_owner','entrances','ga_session_id','checkout_orderdiscountvalue','session_engaged')
        ) AS pivoted_data
)
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
EVENT_DATE,
EVENT_TIMESTAMP ,
EVENT_NAME_ORIGINAL,
USER_SESSION_KEY,
EVENT_BUNDLE_SEQUENCE_ID,
USER_PSEUDO_ID,
ECOMMERCE_TRANSACTION_ID,
DEVICE_CATEGORY,
TRAFFIC_SOURCE_NAME ,
TRAFFIC_SOURCE_MEDIUM ,
TRAFFIC_SOURCE_SOURCE ,
(CASE WHEN EVENT_NAME_ORIGINAL='purchase' THEN ITEMS_QUANTITY ELSE NULL END) AS UNITS_ORDERED,
ITEMS_ITEM_ID,
ITEMS_ITEM_BRAND as FRANCHISE,
ITEMS_ITEM_CATEGORY2 as GENDER,
ITEMS_PRICE_IN_USD ,
ITEMS_ITEM_REVENUE_IN_USD,
EP_PAGE_TYPE,
EP_PAGE_OWNER,
EP_ENTRANCES,
EP_GA_SESSION_ID,
EP_CHECKOUT_ORDERDISCOUNTVALUE,
EP_SESSION_ENGAGED,
TO_NUMBER("'add_to_cart'") as EVENT_ADD_TO_CART,
TO_NUMBER("'login'") as EVENT_LOGIN,
TO_NUMBER("'view_item'") as EVENT_VIEW_ITEM,
TO_NUMBER("'begin_checkout'") as EVENT_BEGIN_CHECKOUT,
TO_NUMBER("'select_item'") as EVENT_SELECT_ITEM,
TO_NUMBER("'first_visit'") as EVENT_FIRST_VISIT,
TO_NUMBER("'session_start'") as EVENT_SESSION_START,
TO_NUMBER("'purchase'") as EVENT_PURCHASE,
TO_NUMBER("'search'") as EVENT_SEARCH,
TO_NUMBER("'remove_from_cart'") as EVENT_REMOVE_FROM_CART,
TO_NUMBER("'user_engagement'") as EVENT_USER_ENGAGEMENT,
TO_NUMBER("'view_item_list'") as EVENT_VIEW_ITEM_LIST,
TO_NUMBER("'page_view'") as EVENT_PAGE_VIEW,
TO_NUMBER("'add_payment_info'") as EVENT_ADD_PAYMENT_INFO,
TO_NUMBER("'add_to_wishlist'") as EVENT_ADD_TO_WISHLIST,
TO_NUMBER("'sign_up_newsletter'") as EVENT_SIGN_UP_NEWSLETTER,
TO_NUMBER("'sign_up'") as EVENT_SIGN_UP,
TO_NUMBER("'view_cart'") as EVENT_VIEW_CART,
TO_NUMBER("'add_shipping_info'") as EVENT_ADD_SHIPPING_INFO,
TO_NUMBER("'navigation'") as EVENT_NAVIGATION,
TO_NUMBER("'select_promotion'") as EVENT_SELECT_PROMOTION,
TO_NUMBER("'view_search_results'") as EVENT_VIEW_SEARCH_RESULTS,
TO_NUMBER("'fetch_user_data'") as EVENT_FETCH_USER_DATA,
TO_NUMBER("'review_order'") as EVENT_REVIEW_ORDER,
TO_NUMBER("'error_404'") as EVENT_ERROR_404,
TO_NUMBER("'sort_applied'") as EVENT_SORT_APPLIED,
TO_NUMBER("'out_of_stock_signup'") as EVENT_OUT_OF_STOCK_SIGNUP,
TO_NUMBER("'filter_added'") as EVENT_FILTER_ADDED,
TO_NUMBER("'no_search_results'") as EVENT_NO_SEARCH_RESULTS,
TO_NUMBER("'form_error'") as EVENT_FORM_ERROR
from FIRST_GA4_FLATTEN AS data_to_pivot_1
PIVOT( MAX(EVENT_NAME_FLAG) FOR EVENT_NAME IN ('view_item','navigation','session_start','purchase','select_promotion','add_to_cart','login','begin_checkout','user_engagement','sign_up','view_search_results','remove_from_cart','fetch_user_data','review_order','error_404','sort_applied','first_visit','select_item','view_item_list','page_view','add_payment_info','add_to_wishlist','sign_up_newsletter','out_of_stock_signup','search','filter_added','no_search_results','add_shipping_info','view_cart','form_error')
) AS pivoted_data

