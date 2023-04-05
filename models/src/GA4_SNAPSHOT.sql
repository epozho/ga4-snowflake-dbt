{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}

SELECT * FROM(
SELECT
	EVENT_DATE ,
	EVENT_TIMESTAMP,
	EVENT_NAME ,
	EVENT_PREVIOUS_TIMESTAMP,
	EVENT_VALUE_IN_USD ,
	EVENT_BUNDLE_SEQUENCE_ID,
	EVENT_SERVER_TIMESTAMP_OFFSET,
	USER_ID ,
	USER_PSEUDO_ID ,
	PRIVACY_INFO_ANALYTICS_STORAGE ,
	PRIVACY_INFO_ADS_STORAGE ,
	PRIVACY_INFO_USES_TRANSIENT_TOKEN ,
	USER_FIRST_TOUCH_TIMESTAMP,
	USER_LTV_REVENUE ,
	USER_LTV_CURRENCY ,
	DEVICE_CATEGORY ,
	DEVICE_MOBILE_BRAND_NAME ,
	DEVICE_MOBILE_MODEL_NAME ,
	DEVICE_MOBILE_MARKETING_NAME ,
	DEVICE_MOBILE_OS_HARDWARE_MODEL ,
	DEVICE_OPERATING_SYSTEM ,
	DEVICE_OPERATING_SYSTEM_VERSION ,
	DEVICE_VENDOR_ID ,
	DEVICE_ADVERTISING_ID ,
	DEVICE_LANGUAGE ,
	DEVICE_IS_LIMITED_AD_TRACKING ,
	DEVICE_TIME_ZONE_OFFSET_SECONDS,
	DEVICE_BROWSER ,
	DEVICE_BROWSER_VERSION ,
	DEVICE_WEB_INFO_BROWSER ,
	DEVICE_WEB_INFO_BROWSER_VERSION ,
	DEVICE_WEB_INFO_HOSTNAME ,
	GEO_CONTINENT ,
	GEO_COUNTRY ,
	GEO_REGION ,
	GEO_CITY ,
	GEO_SUB_CONTINENT ,
	GEO_METRO ,
	APP_INFO_ID ,
	APP_INFO_VERSION ,
	APP_INFO_INSTALL_STORE ,
	APP_INFO_FIREBASE_APP_ID ,
	APP_INFO_INSTALL_SOURCE ,
	TRAFFIC_SOURCE_NAME ,
	TRAFFIC_SOURCE_MEDIUM ,
	TRAFFIC_SOURCE_SOURCE ,
	STREAM_ID ,
	PLATFORM ,
	EVENT_DIMENSIONS_HOSTNAME ,
	ECOMMERCE_TOTAL_ITEM_QUANTITY,
	ECOMMERCE_PURCHASE_REVENUE_IN_USD ,
	ECOMMERCE_PURCHASE_REVENUE ,
	ECOMMERCE_REFUND_VALUE_IN_USD ,
	ECOMMERCE_REFUND_VALUE ,
	ECOMMERCE_SHIPPING_VALUE_IN_USD ,
	ECOMMERCE_SHIPPING_VALUE ,
	ECOMMERCE_TAX_VALUE_IN_USD ,
	ECOMMERCE_TAX_VALUE ,
	ECOMMERCE_UNIQUE_ITEMS,
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
	ITEMS_PRICE_IN_USD ,
	ITEMS_PRICE ,
	ITEMS_QUANTITY,
	ITEMS_ITEM_REVENUE_IN_USD ,
	ITEMS_ITEM_REVENUE ,
	ITEMS_ITEM_REFUND_IN_USD ,
	ITEMS_ITEM_REFUND ,
	ITEMS_COUPON ,
	ITEMS_AFFILIATION ,
	ITEMS_LOCATION_ID ,
	ITEMS_ITEM_LIST_ID ,
	ITEMS_ITEM_LIST_NAME ,
	ITEMS_ITEM_LIST_INDEX ,
	ITEMS_PROMOTION_ID ,
	ITEMS_PROMOTION_NAME ,
	ITEMS_CREATIVE_NAME ,
	ITEMS_CREATIVE_SLOT ,
	TO_VARCHAR("""'url'""") as EP_url,
	TO_VARCHAR("""'srsltid'""") as EP_srsltid,
	TO_VARCHAR("""'page_type'""") as EP_page_type,
	TO_VARCHAR("""'engaged_session_event'""") as EP_engaged_session_event,
	TO_VARCHAR("""'market'""") as EP_market,
	TO_VARCHAR("""'page_owner'""") as EP_page_owner,
	TO_VARCHAR("""'page_referrer'""") as EP_page_referrer,
	TO_VARCHAR("""'payment_type'""") as EP_payment_type,
	TO_VARCHAR("""'debug_mode'""") as EP_debug_mode,
	TO_VARCHAR("""'version_info'""") as EP_version_info,
	TO_VARCHAR("""'gclsrc'""") as EP_gclsrc,
	TO_VARCHAR("""'page_title'""") as EP_page_title,
	TO_VARCHAR("""'cm_mmc2'""") as EP_cm_mmc2,
	TO_VARCHAR("""'medium'""") as EP_medium,
	TO_VARCHAR("""'gclid'""") as EP_gclid,
	TO_VARCHAR("""'coupon'""") as EP_coupon,
	TO_VARCHAR("""'filters_applied'""") as EP_filters_applied,
	TO_VARCHAR("""'currency'""") as EP_currency,
	TO_VARCHAR("""'search_type'""") as EP_search_type,
	TO_VARCHAR("""'firebase_conversion'""") as EP_firebase_conversion,
	TO_VARCHAR("""'ignore_referrer'""") as EP_ignore_referrer,
	TO_VARCHAR("""'content'""") as EP_content,
	TO_VARCHAR("""'method'""") as EP_method,
	TO_VARCHAR("""'campaign_id'""") as EP_campaign_id,
	TO_VARCHAR("""'session_engaged'""") as EP_session_engaged,
	TO_VARCHAR("""'shipping_tier'""") as EP_shipping_tier,
	TO_VARCHAR("""'value'""") as EP_value,
	TO_VARCHAR("""'shipping'""") as EP_shipping,
	TO_VARCHAR("""'anonymize_ip'""") as EP_anonymize_ip,
	TO_VARCHAR("""'entrances'""") as EP_entrances,
	TO_VARCHAR("""'transaction_id'""") as EP_transaction_id,
	TO_VARCHAR("""'page_location'""") as EP_page_location,
	TO_VARCHAR("""'term'""") as EP_term,
	TO_VARCHAR("""'campaign'""") as EP_campaign,
	TO_VARCHAR("""'ga_session_id'""") as EP_ga_session_id,
	TO_VARCHAR("""'search_term'""") as EP_search_term,
	TO_VARCHAR("""'checkout_orderdiscountvalue'""") as EP_checkout_orderdiscountvalue,
	TO_VARCHAR("""'source'""") as EP_source,
	TO_VARCHAR("""'pathname'""") as EP_pathname,
	TO_VARCHAR("""'ga_session_number'""") as EP_ga_session_number,
	TO_VARCHAR("""'engagement_time_msec'""") as EP_engagement_time_msec,
	TO_VARCHAR("""'dclid'""") as EP_dclid,
	TO_VARCHAR("""'affiliation'""") as EP_affiliation,
	TO_VARCHAR("""'tax'""") as EP_tax,
	TO_VARCHAR("'market'") as UP_market,
	TO_VARCHAR("'marketlanguage'") as UP_marketlanguage,
	TO_VARCHAR("'prevenue_28d'") as UP_prevenue_28d,
	TO_VARCHAR("'membership_info'") as UP_membership_info,
	TO_VARCHAR("'user_id'") as UP_user_id,
	TO_VARCHAR("'loginstatus'") as UP_loginstatus
        FROM (
          SELECT
          to_date(EVENT_DATE,'YYYYMMDD') as EVENT_DATE,
           TO_TIMESTAMP(SUBSTR(EVENT_TIMESTAMP,1,13)) as EVENT_TIMESTAMP,
           EVENT_NAME, EVENT_PARAMS_KEY,
                 (CASE
                     WHEN EVENT_PARAMS_VALUE_STRING_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_STRING_VALUE)
                     WHEN EVENT_PARAMS_VALUE_INT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_INT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_FLOAT_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_FLOAT_VALUE)
                     WHEN EVENT_PARAMS_VALUE_DOUBLE_VALUE IS NOT NULL THEN TO_VARIANT(EVENT_PARAMS_VALUE_DOUBLE_VALUE)
                  ELSE NULL
                 END) AS EVENT_PARAMS_VALUE,
                 EVENT_PREVIOUS_TIMESTAMP,EVENT_VALUE_IN_USD, EVENT_BUNDLE_SEQUENCE_ID, EVENT_SERVER_TIMESTAMP_OFFSET,USER_ID,USER_PSEUDO_ID,PRIVACY_INFO_ANALYTICS_STORAGE,PRIVACY_INFO_ADS_STORAGE,PRIVACY_INFO_USES_TRANSIENT_TOKEN,USER_PROPERTIES_KEY,
                 (CASE
                     WHEN USER_PROPERTIES_VALUE_STRING_VALUE IS NOT NULL THEN TO_VARIANT(USER_PROPERTIES_VALUE_STRING_VALUE)
                     WHEN USER_PROPERTIES_VALUE_INT_VALUE IS NOT NULL THEN TO_VARIANT(USER_PROPERTIES_VALUE_INT_VALUE)
                     WHEN USER_PROPERTIES_VALUE_FLOAT_VALUE IS NOT NULL THEN TO_VARIANT(USER_PROPERTIES_VALUE_FLOAT_VALUE)
                     WHEN USER_PROPERTIES_VALUE_DOUBLE_VALUE IS NOT NULL THEN TO_VARIANT(USER_PROPERTIES_VALUE_DOUBLE_VALUE)
                     WHEN USER_PROPERTIES_VALUE_SET_TIMESTAMP_MICROS IS NOT NULL THEN TO_VARIANT(USER_PROPERTIES_VALUE_SET_TIMESTAMP_MICROS)
                  ELSE NULL
                 END) AS USER_PROPERTIES_VALUE,
                 TO_TIMESTAMP(SUBSTR(USER_FIRST_TOUCH_TIMESTAMP,1,13)) as USER_FIRST_TOUCH_TIMESTAMP,
                 user_ltv_revenue , user_ltv_currency , device_category , device_mobile_brand_name , device_mobile_model_name , device_mobile_marketing_name , device_mobile_os_hardware_model , device_operating_system , device_operating_system_version , device_vendor_id , device_advertising_id , device_language , device_is_limited_ad_tracking , device_time_zone_offset_seconds , device_browser , device_browser_version , device_web_info_browser , device_web_info_browser_version , device_web_info_hostname , geo_continent , geo_country , geo_region , geo_city , geo_sub_continent , geo_metro , app_info_id , app_info_version , app_info_install_store , app_info_firebase_app_id , app_info_install_source , traffic_source_name , traffic_source_medium , traffic_source_source , stream_id , platform , event_dimensions_hostname , ecommerce_total_item_quantity , ecommerce_purchase_revenue_in_usd , ecommerce_purchase_revenue , ecommerce_refund_value_in_usd , ecommerce_refund_value , ecommerce_shipping_value_in_usd , ecommerce_shipping_value , ecommerce_tax_value_in_usd , ecommerce_tax_value , ecommerce_unique_items , ecommerce_transaction_id , items_item_id , items_item_name , items_item_brand , items_item_variant , items_item_category , items_item_category2 , items_item_category3 , items_item_category4 , items_item_category5 , items_price_in_usd , items_price , items_quantity , items_item_revenue_in_usd , items_item_revenue , items_item_refund_in_usd , items_item_refund , items_coupon , items_affiliation , items_location_id , items_item_list_id , items_item_list_name , items_item_list_index , items_promotion_id , items_promotion_name , items_creative_name , items_creative_slot
          FROM SPARC_RAW.RBOK_GA.GA4_EVENTS_RAW
        ) AS data_to_pivot
        PIVOT (
          MAX(EVENT_PARAMS_VALUE) FOR EVENT_PARAMS_KEY IN ('url','srsltid','page_type','engaged_session_event','market','page_owner','page_referrer','payment_type','debug_mode','version_info','gclsrc','page_title','cm_mmc2','medium','gclid','coupon','filters_applied','currency','search_type','firebase_conversion','ignore_referrer','content','method','campaign_id','session_engaged','shipping_tier','value','shipping','anonymize_ip','entrances','transaction_id','page_location','term','campaign','ga_session_id','search_term','checkout_orderdiscountvalue','source','pathname','ga_session_number','engagement_time_msec','dclid','affiliation','tax')
        ) AS pivoted_data
        PIVOT (
          MAX(USER_PROPERTIES_VALUE) FOR USER_PROPERTIES_KEY IN ('market','marketlanguage','prevenue_28d','membership_info','user_id','loginstatus')
        ) AS pivoted_data_1
)       
 