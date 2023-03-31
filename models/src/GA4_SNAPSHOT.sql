{{ config(materialized="table", database="SPARC_BASE", schema="ECOM_ANALYTICS") }}

SELECT *
        FROM (
          SELECT EVENT_DATE, EVENT_TIMESTAMP,EVENT_NAME, EVENT_PARAMS_KEY,
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
                 user_first_touch_timestamp , user_ltv_revenue , user_ltv_currency , device_category , device_mobile_brand_name , device_mobile_model_name , device_mobile_marketing_name , device_mobile_os_hardware_model , device_operating_system , device_operating_system_version , device_vendor_id , device_advertising_id , device_language , device_is_limited_ad_tracking , device_time_zone_offset_seconds , device_browser , device_browser_version , device_web_info_browser , device_web_info_browser_version , device_web_info_hostname , geo_continent , geo_country , geo_region , geo_city , geo_sub_continent , geo_metro , app_info_id , app_info_version , app_info_install_store , app_info_firebase_app_id , app_info_install_source , traffic_source_name , traffic_source_medium , traffic_source_source , stream_id , platform , event_dimensions_hostname , ecommerce_total_item_quantity , ecommerce_purchase_revenue_in_usd , ecommerce_purchase_revenue , ecommerce_refund_value_in_usd , ecommerce_refund_value , ecommerce_shipping_value_in_usd , ecommerce_shipping_value , ecommerce_tax_value_in_usd , ecommerce_tax_value , ecommerce_unique_items , ecommerce_transaction_id , items_item_id , items_item_name , items_item_brand , items_item_variant , items_item_category , items_item_category2 , items_item_category3 , items_item_category4 , items_item_category5 , items_price_in_usd , items_price , items_quantity , items_item_revenue_in_usd , items_item_revenue , items_item_refund_in_usd , items_item_refund , items_coupon , items_affiliation , items_location_id , items_item_list_id , items_item_list_name , items_item_list_index , items_promotion_id , items_promotion_name , items_creative_name , items_creative_slot
          FROM SPARC_RAW.RBOK_GA.GA4_EVENTS_RAW
        ) AS data_to_pivot
        PIVOT (
          MAX(EVENT_PARAMS_VALUE) FOR EVENT_PARAMS_KEY IN ('url','srsltid','page_type','engaged_session_event','market','page_owner','page_referrer','payment_type','debug_mode','version_info','gclsrc','page_title','cm_mmc2','medium','gclid','coupon','filters_applied','currency','search_type','firebase_conversion','ignore_referrer','content','method','campaign_id','session_engaged','shipping_tier','value','shipping','anonymize_ip','entrances','transaction_id','page_location','term','campaign','ga_session_id','search_term','checkout_orderdiscountvalue','source','pathname','ga_session_number','engagement_time_msec','dclid','affiliation','tax')
        ) AS pivoted_data
        PIVOT (
          MAX(USER_PROPERTIES_VALUE) FOR USER_PROPERTIES_KEY IN ('market','marketlanguage','prevenue_28d','membership_info','user_id','loginstatus')
        ) AS pivoted_data_1