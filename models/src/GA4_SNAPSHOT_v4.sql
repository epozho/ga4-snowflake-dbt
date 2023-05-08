{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}

Select * from(
Select
EVENT_DATE,
EVENT_TIMESTAMP ,
EVENT_NAME_ORIGINAL,
USER_SESSION_KEY,
USER_PSEUDO_ID,
DEVICE_CATEGORY,
TRAFFIC_SOURCE_NAME,
TRAFFIC_SOURCE_MEDIUM,
TRAFFIC_SOURCE_SOURCE,
ECOMMERCE_TOTAL_ITEM_QUANTITY,
ECOMMERCE_PURCHASE_REVENUE_IN_USD,
ECOMMERCE_PURCHASE_REVENUE,
ECOMMERCE_SHIPPING_VALUE_IN_USD,
ECOMMERCE_TAX_VALUE_IN_USD,
ECOMMERCE_TRANSACTION_ID,
ITEMS_ITEM_ID,
ITEMS_ITEM_NAME,
ITEMS_ITEM_BRAND,
ITEMS_ITEM_VARIANT,
ITEMS_ITEM_CATEGORY,
ITEMS_ITEM_CATEGORY2
ITEMS_ITEM_CATEGORY3,
ITEMS_ITEM_CATEGORY4,
ITEMS_ITEM_CATEGORY5,
EP_PAGE_TYPE,
EP_ENGAGED_SESSION_EVENT,
EP_PAGE_OWNER,
EP_MEDIUM,
EP_FIREBASE_CONVERSION,
EP_SESSION_ENGAGED,
EP_ENTRANCES,
EP_TRANSACTION_ID,
EP_CAMPAIGN,
EP_GA_SESSION_ID,
EP_CHECKOUT_ORDERDISCOUNTVALUE,
EP_SOURCE,
EP_GA_SESSION_NUMBER,
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
TO_NUMBER("'add_shipping_info'") as Event_add_shipping_info
from
(Select
    EVENT_DATE,
	EVENT_TIMESTAMP,
    EVENT_NAME as EVENT_NAME_ORIGINAL,
	EVENT_NAME,
    1 as EVENT_NAME_FLAG,
    CONCAT(USER_PSEUDO_ID,'|',EP_GA_SESSION_ID) as USER_SESSION_KEY,
	USER_PSEUDO_ID,
	DEVICE_CATEGORY,
	TRAFFIC_SOURCE_NAME,
	TRAFFIC_SOURCE_MEDIUM,
	TRAFFIC_SOURCE_SOURCE,
	ECOMMERCE_TOTAL_ITEM_QUANTITY,
	ECOMMERCE_PURCHASE_REVENUE_IN_USD,
	ECOMMERCE_PURCHASE_REVENUE,
	ECOMMERCE_SHIPPING_VALUE_IN_USD,
	ECOMMERCE_TAX_VALUE_IN_USD,
	ECOMMERCE_TRANSACTION_ID,
	ITEMS_ITEM_ID,
	ITEMS_ITEM_NAME,
	ITEMS_ITEM_BRAND,
	ITEMS_ITEM_VARIANT,
	ITEMS_ITEM_CATEGORY,
	ITEMS_ITEM_CATEGORY2,
	ITEMS_ITEM_CATEGORY3,
	ITEMS_ITEM_CATEGORY4,
	ITEMS_ITEM_CATEGORY5,
	EP_PAGE_TYPE,
	EP_ENGAGED_SESSION_EVENT,
	EP_PAGE_OWNER,
	EP_MEDIUM,
	EP_FIREBASE_CONVERSION,
	EP_SESSION_ENGAGED,
	EP_ENTRANCES,
	EP_TRANSACTION_ID,
	EP_CAMPAIGN,
	EP_GA_SESSION_ID,
	EP_CHECKOUT_ORDERDISCOUNTVALUE,
	EP_SOURCE,
	EP_GA_SESSION_NUMBER
from 
    (
        SELECT
	    EVENT_DATE ,
	    EVENT_TIMESTAMP,
	    EVENT_NAME ,
	    USER_PSEUDO_ID ,
	    DEVICE_CATEGORY ,
	    TRAFFIC_SOURCE_NAME ,
	    TRAFFIC_SOURCE_MEDIUM ,
	    TRAFFIC_SOURCE_SOURCE ,
	    ECOMMERCE_TOTAL_ITEM_QUANTITY,
	    ECOMMERCE_PURCHASE_REVENUE_IN_USD ,
	    ECOMMERCE_PURCHASE_REVENUE ,
	    ECOMMERCE_SHIPPING_VALUE_IN_USD ,
	    ECOMMERCE_TAX_VALUE_IN_USD ,
	    ECOMMERCE_TRANSACTION_ID ,
	    ITEMS_ITEM_ID ,
	    ITEMS_ITEM_NAME ,
	    ITEMS_ITEM_BRAND ,
	    ITEMS_ITEM_VARIANT ,
        ITEMS_ITEM_CATEGORY ,
        ITEMS_ITEM_CATEGORY2 ,
        ITEMS_ITEM_CATEGORY3 ,
        ITEMS_ITEM_CATEGORY4 ,
        ITEMS_ITEM_CATEGORY5 ,
        TO_VARCHAR("'page_type'") as EP_page_type,
        TO_NUMBER("'engaged_session_event'") as EP_engaged_session_event,
        TO_VARCHAR("'page_owner'") as EP_page_owner,
        TO_VARCHAR("'medium'") as EP_medium,
        TO_NUMBER("'firebase_conversion'") as EP_firebase_conversion,
        TO_NUMBER("'session_engaged'") as EP_session_engaged,
        TO_NUMBER("'entrances'") as EP_entrances,
        TO_VARCHAR("'transaction_id'") as EP_transaction_id,
        TO_VARCHAR("'campaign'") as EP_campaign,
	    TO_VARCHAR("'ga_session_id'") as EP_ga_session_id,
	    TO_NUMBER("'checkout_orderdiscountvalue'",10,2) as EP_checkout_orderdiscountvalue,
	    TO_VARCHAR("'source'") as EP_source,
	    TO_NUMBER("'ga_session_number'") as EP_ga_session_number
        FROM (
                SELECT
                TO_DATE(EVENT_DATE,'YYYYMMDD') as EVENT_DATE,
                TO_TIMESTAMP(SUBSTR(EVENT_TIMESTAMP,1,13)) as EVENT_TIMESTAMP,
                EVENT_NAME, EVENT_PARAMS_KEY,
                 (CASE
                     WHEN EVENT_PARAMS_VALUE_STRING_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_STRING_VALUE)
                     WHEN EVENT_PARAMS_VALUE_INT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_INT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_FLOAT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_FLOAT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_DOUBLE_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_DOUBLE_VALUE)
                  ELSE NULL
                 END) AS EVENT_PARAMS_VALUE,
                 USER_PSEUDO_ID,
                 device_category , traffic_source_name , traffic_source_medium , traffic_source_source, ecommerce_total_item_quantity , ecommerce_purchase_revenue_in_usd , ecommerce_purchase_revenue , ecommerce_shipping_value_in_usd, ecommerce_tax_value_in_usd, ecommerce_transaction_id , items_item_id , items_item_name , items_item_brand , items_item_variant , items_item_category , items_item_category2 , items_item_category3 , items_item_category4 , items_item_category5
          FROM SPARC_BASE.ECOM_ANALYTICS.GA4_EVENTS_RAW_BACKUP
        ) AS data_to_pivot
        PIVOT (
          MAX(EVENT_PARAMS_VALUE) FOR EVENT_PARAMS_KEY IN ('page_type','engaged_session_event','page_owner','medium','firebase_conversion','session_engaged','entrances','transaction_id','campaign','ga_session_id','checkout_orderdiscountvalue','source','ga_session_number')
        ) AS pivoted_data
    )
) as data_to_pivot_1
PIVOT( MAX(EVENT_NAME_FLAG) FOR EVENT_NAME IN ('add_to_cart','login','view_item','begin_checkout','select_item','first_visit','session_start','purchase','search','remove_from_cart','user_engagement','view_item_list','page_view','add_payment_info','add_to_wishlist','sign_up_newsletter','sign_up','view_cart','add_shipping_info')
) AS pivoted_data
)