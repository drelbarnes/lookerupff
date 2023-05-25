view: branch_events {
  derived_table: {
    sql:
      SELECT
      SAFE_CAST(_id AS STRING) AS _id,
      SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
      SAFE_CAST(attributed AS STRING) AS attributed,
      SAFE_CAST(content_items AS STRING) AS content_items,
      SAFE_CAST(context_library_name AS STRING) AS context_library_name,
      SAFE_CAST(context_library_version AS STRING) AS context_library_version,
      SAFE_CAST(cross_device_ott AS STRING) AS cross_device_ott,
      SAFE_CAST(custom_data_gateway AS STRING) AS custom_data_gateway,
      SAFE_CAST(custom_data_segment_anonymous_id AS STRING) AS custom_data_segment_anonymous_id,
      SAFE_CAST(custom_data_skan_time_window AS STRING) AS custom_data_skan_time_window,
      SAFE_CAST(days_from_last_attributed_touch_to_event AS INT64) AS days_from_last_attributed_touch_to_event,
      SAFE_CAST(deep_linked AS STRING) AS deep_linked,
      SAFE_CAST(event AS STRING) AS event,
      SAFE_CAST(event_days_from_timestamp AS INT64) AS event_days_from_timestamp,
      SAFE_CAST(event_text AS STRING) AS event_text,
      SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
      SAFE_CAST(existing_user AS STRING) AS existing_user,
      SAFE_CAST(first_event_for_user AS STRING) AS first_event_for_user,
      SAFE_CAST(hours_from_last_attributed_touch_to_event AS INT64) AS hours_from_last_attributed_touch_to_event,
      SAFE_CAST(id AS STRING) AS id,
      SAFE_CAST(install_activity_attributed AS BOOLEAN) AS install_activity_attributed,
      SAFE_CAST(install_activity_data_country_code AS STRING) AS install_activity_data_country_code,
      SAFE_CAST(install_activity_event_name AS STRING) AS install_activity_event_name,
      SAFE_CAST(install_activity_timestamp AS INT64) AS install_activity_timestamp,
      SAFE_CAST(install_activity_touch_data_dollar_3p AS STRING) AS install_activity_touch_data_dollar_3p,
      SAFE_CAST(install_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS install_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(install_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS install_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(install_activity_touch_data_tilde_advertising_partner_name AS STRING) AS install_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_3p AS STRING) AS last_attributed_touch_data_3p,
      SAFE_CAST(last_attributed_touch_data_ad_id AS STRING) AS last_attributed_touch_data_ad_id,
      SAFE_CAST(last_attributed_touch_data_ad_name AS STRING) AS last_attributed_touch_data_ad_name,
      SAFE_CAST(last_attributed_touch_data_ad_objective_name AS STRING) AS last_attributed_touch_data_ad_objective_name,
      SAFE_CAST(last_attributed_touch_data_ad_set_id AS STRING) AS last_attributed_touch_data_ad_set_id,
      SAFE_CAST(last_attributed_touch_data_ad_set_name AS STRING) AS last_attributed_touch_data_ad_set_name,
      SAFE_CAST(last_attributed_touch_data_advertising_account_id AS STRING) AS last_attributed_touch_data_advertising_account_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_id AS STRING) AS last_attributed_touch_data_advertising_partner_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_name AS STRING) AS last_attributed_touch_data_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_android_passive_deepview AS STRING) AS last_attributed_touch_data_android_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_android_url AS STRING) AS last_attributed_touch_data_android_url,
      SAFE_CAST(last_attributed_touch_data_api_open_click AS STRING) AS last_attributed_touch_data_api_open_click,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution AS BOOLEAN) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date AS TIMESTAMP) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id,
      SAFE_CAST(last_attributed_touch_data_branch_ad_format AS STRING) AS last_attributed_touch_data_branch_ad_format,
      SAFE_CAST(last_attributed_touch_data_campaign AS STRING) AS last_attributed_touch_data_campaign,
      SAFE_CAST(last_attributed_touch_data_campaign_id AS STRING) AS last_attributed_touch_data_campaign_id,
      SAFE_CAST(last_attributed_touch_data_campaign_type AS STRING) AS last_attributed_touch_data_campaign_type,
      SAFE_CAST(last_attributed_touch_data_canonical_url AS STRING) AS last_attributed_touch_data_canonical_url,
      SAFE_CAST(last_attributed_touch_data_channel AS STRING) AS last_attributed_touch_data_channel,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser_version AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser_version,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_is_mobile AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_is_mobile,
      SAFE_CAST(last_attributed_touch_data_click_timestamp AS INT64) AS last_attributed_touch_data_click_timestamp,
      SAFE_CAST(last_attributed_touch_data_collection AS STRING) AS last_attributed_touch_data_collection,
      SAFE_CAST(last_attributed_touch_data_conversion_type AS STRING) AS last_attributed_touch_data_conversion_type,
      SAFE_CAST(last_attributed_touch_data_country_or_region AS STRING) AS last_attributed_touch_data_country_or_region,
      SAFE_CAST(last_attributed_touch_data_creative_id AS STRING) AS last_attributed_touch_data_creative_id,
      SAFE_CAST(last_attributed_touch_data_creative_name AS STRING) AS last_attributed_touch_data_creative_name,
      SAFE_CAST(last_attributed_touch_data_desktop_url AS STRING) AS last_attributed_touch_data_desktop_url,
      SAFE_CAST(last_attributed_touch_data_device_brand_model AS STRING) AS last_attributed_touch_data_device_brand_model,
      SAFE_CAST(last_attributed_touch_data_device_brand_name AS STRING) AS last_attributed_touch_data_device_brand_name,
      SAFE_CAST(last_attributed_touch_data_device_os AS STRING) AS last_attributed_touch_data_device_os,
      SAFE_CAST(last_attributed_touch_data_device_os_version AS STRING) AS last_attributed_touch_data_device_os_version,
      SAFE_CAST(last_attributed_touch_data_fb_data_terms_not_signed AS STRING) AS last_attributed_touch_data_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_fbclid AS STRING) AS last_attributed_touch_data_fbclid,
      SAFE_CAST(last_attributed_touch_data_feature AS STRING) AS last_attributed_touch_data_feature,
      SAFE_CAST(last_attributed_touch_data_geo_country_code AS STRING) AS last_attributed_touch_data_geo_country_code,
      SAFE_CAST(last_attributed_touch_data_ios_passive_deepview AS STRING) AS last_attributed_touch_data_ios_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_ios_url AS STRING) AS last_attributed_touch_data_ios_url,
      SAFE_CAST(last_attributed_touch_data_is_mobile_data_terms_signed AS STRING) AS last_attributed_touch_data_is_mobile_data_terms_signed,
      SAFE_CAST(last_attributed_touch_data_keyword AS STRING) AS last_attributed_touch_data_keyword,
      SAFE_CAST(last_attributed_touch_data_keyword_id AS STRING) AS last_attributed_touch_data_keyword_id,
      SAFE_CAST(last_attributed_touch_data_keyword_match_type AS STRING) AS last_attributed_touch_data_keyword_match_type,
      SAFE_CAST(last_attributed_touch_data_link_title AS STRING) AS last_attributed_touch_data_link_title,
      SAFE_CAST(last_attributed_touch_data_link_type AS STRING) AS last_attributed_touch_data_link_type,
      SAFE_CAST(last_attributed_touch_data_one_time_use AS STRING) AS last_attributed_touch_data_one_time_use,
      SAFE_CAST(last_attributed_touch_data_organic_search_url AS STRING) AS last_attributed_touch_data_organic_search_url,
      SAFE_CAST(last_attributed_touch_data_secondary_ad_format AS STRING) AS last_attributed_touch_data_secondary_ad_format,
      SAFE_CAST(last_attributed_touch_data_secondary_publisher AS STRING) AS last_attributed_touch_data_secondary_publisher,
      SAFE_CAST(last_attributed_touch_data_tags AS STRING) AS last_attributed_touch_data_tags,
      SAFE_CAST(last_attributed_touch_data_touch_id AS STRING) AS last_attributed_touch_data_touch_id,
      SAFE_CAST(last_attributed_touch_data_touch_subtype AS STRING) AS last_attributed_touch_data_touch_subtype,
      SAFE_CAST(last_attributed_touch_data_tune_publisher_name AS STRING) AS last_attributed_touch_data_tune_publisher_name,
      SAFE_CAST(last_attributed_touch_data_url AS STRING) AS last_attributed_touch_data_url,
      SAFE_CAST(last_attributed_touch_data_user_data_ip AS STRING) AS last_attributed_touch_data_user_data_ip,
      SAFE_CAST(last_attributed_touch_data_user_data_user_agent AS STRING) AS last_attributed_touch_data_user_data_user_agent,
      SAFE_CAST(last_attributed_touch_data_via_features AS STRING) AS last_attributed_touch_data_via_features,
      SAFE_CAST(last_attributed_touch_timestamp AS INT64) AS last_attributed_touch_timestamp,
      SAFE_CAST(last_attributed_touch_type AS STRING) AS last_attributed_touch_type,
      SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
      SAFE_CAST(minutes_from_last_attributed_touch_to_event AS INT64) AS minutes_from_last_attributed_touch_to_event,
      SAFE_CAST(name AS STRING) AS name,
      SAFE_CAST(origin AS STRING) AS origin,
      SAFE_CAST(original_timestamp AS TIMESTAMP) AS original_timestamp,
      SAFE_CAST(ott AS STRING) AS ott,
      SAFE_CAST(received_at AS TIMESTAMP) AS received_at,
      SAFE_CAST(reengagement_activity_attributed AS BOOLEAN) AS reengagement_activity_attributed,
      SAFE_CAST(reengagement_activity_data_country_code AS STRING) AS reengagement_activity_data_country_code,
      SAFE_CAST(reengagement_activity_event_name AS STRING) AS reengagement_activity_event_name,
      SAFE_CAST(reengagement_activity_timestamp AS INT64) AS reengagement_activity_timestamp,
      SAFE_CAST(reengagement_activity_touch_data_dollar_3p AS STRING) AS reengagement_activity_touch_data_dollar_3p,
      SAFE_CAST(reengagement_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(reengagement_activity_touch_data_tilde_advertising_partner_name AS STRING) AS reengagement_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(seconds_from_install_to_event AS INT64) AS seconds_from_install_to_event,
      SAFE_CAST(seconds_from_last_attributed_touch_to_event AS INT64) AS seconds_from_last_attributed_touch_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_store_install_begin,
      SAFE_CAST(sent_at AS TIMESTAMP) AS sent_at,
      SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
      SAFE_CAST(user_data_aaid AS STRING) AS user_data_aaid,
      SAFE_CAST(user_data_android_id AS STRING) AS user_data_android_id,
      SAFE_CAST(user_data_app_version AS STRING) AS user_data_app_version,
      SAFE_CAST(user_data_brand AS STRING) AS user_data_brand,
      SAFE_CAST(user_data_build AS STRING) AS user_data_build,
      SAFE_CAST(user_data_carrier_name AS STRING) AS user_data_carrier_name,
      SAFE_CAST(user_data_cpu_type AS STRING) AS user_data_cpu_type,
      SAFE_CAST(user_data_developer_identity AS STRING) AS user_data_developer_identity,
      SAFE_CAST(user_data_disable_ad_network_callouts AS BOOLEAN) AS user_data_disable_ad_network_callouts,
      SAFE_CAST(user_data_environment AS STRING) AS user_data_environment,
      SAFE_CAST(user_data_geo_city_code AS INT64) AS user_data_geo_city_code,
      SAFE_CAST(user_data_geo_city_en AS STRING) AS user_data_geo_city_en,
      SAFE_CAST(user_data_geo_continent_code AS STRING) AS user_data_geo_continent_code,
      SAFE_CAST(user_data_geo_country_code AS STRING) AS user_data_geo_country_code,
      SAFE_CAST(user_data_geo_country_en AS STRING) AS user_data_geo_country_en,
      SAFE_CAST(user_data_geo_dma_code AS INT64) AS user_data_geo_dma_code,
      SAFE_CAST(user_data_geo_lat AS FLOAT64) AS user_data_geo_lat,
      SAFE_CAST(user_data_geo_lon AS FLOAT64) AS user_data_geo_lon,
      SAFE_CAST(user_data_geo_region_code AS STRING) AS user_data_geo_region_code,
      SAFE_CAST(user_data_geo_region_en AS STRING) AS user_data_geo_region_en,
      SAFE_CAST(user_data_http_referrer AS STRING) AS user_data_http_referrer,
      SAFE_CAST(user_data_idfa AS STRING) AS user_data_idfa,
      SAFE_CAST(user_data_idfv AS STRING) AS user_data_idfv,
      SAFE_CAST(user_data_internet_connection_type AS STRING) AS user_data_internet_connection_type,
      SAFE_CAST(user_data_ip AS STRING) AS user_data_ip,
      SAFE_CAST(user_data_language AS STRING) AS user_data_language,
      SAFE_CAST(user_data_limit_ad_tracking AS BOOLEAN) AS user_data_limit_ad_tracking,
      SAFE_CAST(user_data_model AS STRING) AS user_data_model,
      SAFE_CAST(user_data_opted_in AS BOOLEAN) AS user_data_opted_in,
      SAFE_CAST(user_data_opted_in_status AS STRING) AS user_data_opted_in_status,
      SAFE_CAST(user_data_os AS STRING) AS user_data_os,
      SAFE_CAST(user_data_os_version AS STRING) AS user_data_os_version,
      SAFE_CAST(user_data_os_version_android AS STRING) AS user_data_os_version_android,
      SAFE_CAST(user_data_past_cross_platform_ids AS STRING) AS user_data_past_cross_platform_ids,
      SAFE_CAST(user_data_platform AS STRING) AS user_data_platform,
      SAFE_CAST(user_data_private_relay AS BOOLEAN) AS user_data_private_relay,
      SAFE_CAST(user_data_prob_cross_platform_ids AS STRING) AS user_data_prob_cross_platform_ids,
      SAFE_CAST(user_data_screen_height AS INT64) AS user_data_screen_height,
      SAFE_CAST(user_data_screen_width AS INT64) AS user_data_screen_width,
      SAFE_CAST(user_data_sdk_version AS STRING) AS user_data_sdk_version,
      SAFE_CAST(user_data_user_agent AS STRING) AS user_data_user_agent,
      SAFE_CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
      SAFE_CAST(last_attributed_touch_data_always_deeplink AS STRING) AS last_attributed_touch_data_always_deeplink,
      SAFE_CAST(last_attributed_touch_data_dollar_cross_device AS BOOLEAN) AS last_attributed_touch_data_dollar_cross_device,
      SAFE_CAST(last_attributed_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS last_attributed_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS last_attributed_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(last_attributed_touch_data_fallback_url AS STRING) AS last_attributed_touch_data_fallback_url,
      SAFE_CAST(last_attributed_touch_data_gclid AS STRING) AS last_attributed_touch_data_gclid,
      SAFE_CAST(last_attributed_touch_data_placement AS STRING) AS last_attributed_touch_data_placement,
      SAFE_CAST(last_attributed_touch_data_view_through AS BOOLEAN) AS last_attributed_touch_data_view_through,
      SAFE_CAST(last_attributed_touch_data_view_time AS INT64) AS last_attributed_touch_data_view_time,
      SAFE_CAST(last_attributed_touch_data_view_timestamp AS STRING) AS last_attributed_touch_data_view_timestamp,
      SAFE_CAST(store_install_begin_timestamp AS INT64) AS store_install_begin_timestamp,
      SAFE_CAST(referrer_click_timestamp AS INT64) AS referrer_click_timestamp,
      SAFE_CAST(custom_data_opt_in AS STRING) AS custom_data_opt_in,
      SAFE_CAST(days_from_install_to_opt_in AS INT64) AS days_from_install_to_opt_in,
      SAFE_CAST(last_attributed_touch_data_domain AS STRING) AS last_attributed_touch_data_domain,
      SAFE_CAST(last_attributed_touch_data_gb AS STRING) AS last_attributed_touch_data_gb,
      SAFE_CAST(last_attributed_touch_data_matching_ttl_s AS STRING) AS last_attributed_touch_data_matching_ttl_s,
      SAFE_CAST(last_attributed_touch_data_referrer AS STRING) AS last_attributed_touch_data_referrer,
      SAFE_CAST(last_attributed_touch_data_platform_source AS STRING) AS last_attributed_touch_data_platform_source,
      SAFE_CAST(last_attributed_touch_data_creation_source AS INT64) AS last_attributed_touch_data_creation_source,
      SAFE_CAST(last_attributed_touch_data_id AS INT64) AS last_attributed_touch_data_id,
      SAFE_CAST(last_attributed_touch_data_marketing AS BOOLEAN) AS last_attributed_touch_data_marketing,
      SAFE_CAST(last_attributed_touch_data_marketing_title AS STRING) AS last_attributed_touch_data_marketing_title,
      SAFE_CAST(last_attributed_touch_data_og_app_id AS STRING) AS last_attributed_touch_data_og_app_id,
      SAFE_CAST(last_attributed_touch_data_og_description AS STRING) AS last_attributed_touch_data_og_description,
      SAFE_CAST(last_attributed_touch_data_og_image_url AS STRING) AS last_attributed_touch_data_og_image_url,
      SAFE_CAST(last_attributed_touch_data_og_title AS STRING) AS last_attributed_touch_data_og_title,
      SAFE_CAST(last_attributed_touch_data_og_type AS STRING) AS last_attributed_touch_data_og_type,
      SAFE_CAST(last_attributed_touch_data_twitter_card AS STRING) AS last_attributed_touch_data_twitter_card,
      SAFE_CAST(last_attributed_touch_data_twitter_description AS STRING) AS last_attributed_touch_data_twitter_description,
      SAFE_CAST(last_attributed_touch_data_twitter_title AS STRING) AS last_attributed_touch_data_twitter_title,
      SAFE_CAST(last_attributed_touch_data_click_id AS STRING) AS last_attributed_touch_data_click_id,
      SAFE_CAST(last_attributed_touch_data_msclkid AS STRING) AS last_attributed_touch_data_msclkid,
      SAFE_CAST(last_attributed_touch_data_web_only AS STRING) AS last_attributed_touch_data_web_only,
      SAFE_CAST(null AS STRING) AS event_data_currency,
      SAFE_CAST(null AS STRING) AS event_data_description,
      SAFE_CAST(null AS INT64) AS event_data_exchange_rate,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue,
      SAFE_CAST(null AS INT64) AS event_data_revenue_in_local_currency,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue_in_usd,
      SAFE_CAST(null AS STRING) AS event_data_transaction_id,
      SAFE_CAST(null AS STRING) AS user_data_browser
      FROM
      branch_io_v2.open
      union all
      SELECT
      SAFE_CAST(_id AS STRING) AS _id,
      SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
      SAFE_CAST(attributed AS STRING) AS attributed,
      SAFE_CAST(content_items AS STRING) AS content_items,
      SAFE_CAST(context_library_name AS STRING) AS context_library_name,
      SAFE_CAST(context_library_version AS STRING) AS context_library_version,
      SAFE_CAST(cross_device_ott AS STRING) AS cross_device_ott,
      SAFE_CAST(custom_data_gateway AS STRING) AS custom_data_gateway,
      SAFE_CAST(custom_data_segment_anonymous_id AS STRING) AS custom_data_segment_anonymous_id,
      SAFE_CAST(custom_data_skan_time_window AS STRING) AS custom_data_skan_time_window,
      SAFE_CAST(days_from_last_attributed_touch_to_event AS INT64) AS days_from_last_attributed_touch_to_event,
      SAFE_CAST(deep_linked AS STRING) AS deep_linked,
      SAFE_CAST(event AS STRING) AS event,
      SAFE_CAST(event_days_from_timestamp AS INT64) AS event_days_from_timestamp,
      SAFE_CAST(event_text AS STRING) AS event_text,
      SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
      SAFE_CAST(existing_user AS STRING) AS existing_user,
      SAFE_CAST(first_event_for_user AS STRING) AS first_event_for_user,
      SAFE_CAST(hours_from_last_attributed_touch_to_event AS INT64) AS hours_from_last_attributed_touch_to_event,
      SAFE_CAST(id AS STRING) AS id,
      SAFE_CAST(install_activity_attributed AS BOOLEAN) AS install_activity_attributed,
      SAFE_CAST(install_activity_data_country_code AS STRING) AS install_activity_data_country_code,
      SAFE_CAST(install_activity_event_name AS STRING) AS install_activity_event_name,
      SAFE_CAST(install_activity_timestamp AS INT64) AS install_activity_timestamp,
      SAFE_CAST(install_activity_touch_data_dollar_3p AS STRING) AS install_activity_touch_data_dollar_3p,
      SAFE_CAST(install_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS install_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(install_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS install_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(install_activity_touch_data_tilde_advertising_partner_name AS STRING) AS install_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_3p AS STRING) AS last_attributed_touch_data_3p,
      SAFE_CAST(last_attributed_touch_data_ad_id AS STRING) AS last_attributed_touch_data_ad_id,
      SAFE_CAST(last_attributed_touch_data_ad_name AS STRING) AS last_attributed_touch_data_ad_name,
      SAFE_CAST(last_attributed_touch_data_ad_objective_name AS STRING) AS last_attributed_touch_data_ad_objective_name,
      SAFE_CAST(last_attributed_touch_data_ad_set_id AS STRING) AS last_attributed_touch_data_ad_set_id,
      SAFE_CAST(last_attributed_touch_data_ad_set_name AS STRING) AS last_attributed_touch_data_ad_set_name,
      SAFE_CAST(last_attributed_touch_data_advertising_account_id AS STRING) AS last_attributed_touch_data_advertising_account_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_id AS STRING) AS last_attributed_touch_data_advertising_partner_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_name AS STRING) AS last_attributed_touch_data_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_android_passive_deepview AS STRING) AS last_attributed_touch_data_android_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_android_url AS STRING) AS last_attributed_touch_data_android_url,
      SAFE_CAST(last_attributed_touch_data_api_open_click AS STRING) AS last_attributed_touch_data_api_open_click,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution AS BOOLEAN) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date AS TIMESTAMP) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id,
      SAFE_CAST(last_attributed_touch_data_branch_ad_format AS STRING) AS last_attributed_touch_data_branch_ad_format,
      SAFE_CAST(last_attributed_touch_data_campaign AS STRING) AS last_attributed_touch_data_campaign,
      SAFE_CAST(last_attributed_touch_data_campaign_id AS STRING) AS last_attributed_touch_data_campaign_id,
      SAFE_CAST(last_attributed_touch_data_campaign_type AS STRING) AS last_attributed_touch_data_campaign_type,
      SAFE_CAST(last_attributed_touch_data_canonical_url AS STRING) AS last_attributed_touch_data_canonical_url,
      SAFE_CAST(last_attributed_touch_data_channel AS STRING) AS last_attributed_touch_data_channel,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser_version AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser_version,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_is_mobile AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_is_mobile,
      SAFE_CAST(last_attributed_touch_data_click_timestamp AS INT64) AS last_attributed_touch_data_click_timestamp,
      SAFE_CAST(last_attributed_touch_data_collection AS STRING) AS last_attributed_touch_data_collection,
      SAFE_CAST(last_attributed_touch_data_conversion_type AS STRING) AS last_attributed_touch_data_conversion_type,
      SAFE_CAST(last_attributed_touch_data_country_or_region AS STRING) AS last_attributed_touch_data_country_or_region,
      SAFE_CAST(last_attributed_touch_data_creative_id AS STRING) AS last_attributed_touch_data_creative_id,
      SAFE_CAST(last_attributed_touch_data_creative_name AS STRING) AS last_attributed_touch_data_creative_name,
      SAFE_CAST(last_attributed_touch_data_desktop_url AS STRING) AS last_attributed_touch_data_desktop_url,
      SAFE_CAST(last_attributed_touch_data_device_brand_model AS STRING) AS last_attributed_touch_data_device_brand_model,
      SAFE_CAST(last_attributed_touch_data_device_brand_name AS STRING) AS last_attributed_touch_data_device_brand_name,
      SAFE_CAST(last_attributed_touch_data_device_os AS STRING) AS last_attributed_touch_data_device_os,
      SAFE_CAST(last_attributed_touch_data_device_os_version AS STRING) AS last_attributed_touch_data_device_os_version,
      SAFE_CAST(last_attributed_touch_data_fb_data_terms_not_signed AS STRING) AS last_attributed_touch_data_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_fbclid AS STRING) AS last_attributed_touch_data_fbclid,
      SAFE_CAST(last_attributed_touch_data_feature AS STRING) AS last_attributed_touch_data_feature,
      SAFE_CAST(last_attributed_touch_data_geo_country_code AS STRING) AS last_attributed_touch_data_geo_country_code,
      SAFE_CAST(last_attributed_touch_data_ios_passive_deepview AS STRING) AS last_attributed_touch_data_ios_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_ios_url AS STRING) AS last_attributed_touch_data_ios_url,
      SAFE_CAST(last_attributed_touch_data_is_mobile_data_terms_signed AS STRING) AS last_attributed_touch_data_is_mobile_data_terms_signed,
      SAFE_CAST(last_attributed_touch_data_keyword AS STRING) AS last_attributed_touch_data_keyword,
      SAFE_CAST(last_attributed_touch_data_keyword_id AS STRING) AS last_attributed_touch_data_keyword_id,
      SAFE_CAST(last_attributed_touch_data_keyword_match_type AS STRING) AS last_attributed_touch_data_keyword_match_type,
      SAFE_CAST(last_attributed_touch_data_link_title AS STRING) AS last_attributed_touch_data_link_title,
      SAFE_CAST(last_attributed_touch_data_link_type AS STRING) AS last_attributed_touch_data_link_type,
      SAFE_CAST(last_attributed_touch_data_one_time_use AS STRING) AS last_attributed_touch_data_one_time_use,
      SAFE_CAST(last_attributed_touch_data_organic_search_url AS STRING) AS last_attributed_touch_data_organic_search_url,
      SAFE_CAST(last_attributed_touch_data_secondary_ad_format AS STRING) AS last_attributed_touch_data_secondary_ad_format,
      SAFE_CAST(last_attributed_touch_data_secondary_publisher AS STRING) AS last_attributed_touch_data_secondary_publisher,
      SAFE_CAST(last_attributed_touch_data_tags AS STRING) AS last_attributed_touch_data_tags,
      SAFE_CAST(last_attributed_touch_data_touch_id AS STRING) AS last_attributed_touch_data_touch_id,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_touch_subtype,
      SAFE_CAST(last_attributed_touch_data_tune_publisher_name AS STRING) AS last_attributed_touch_data_tune_publisher_name,
      SAFE_CAST(last_attributed_touch_data_url AS STRING) AS last_attributed_touch_data_url,
      SAFE_CAST(last_attributed_touch_data_user_data_ip AS STRING) AS last_attributed_touch_data_user_data_ip,
      SAFE_CAST(last_attributed_touch_data_user_data_user_agent AS STRING) AS last_attributed_touch_data_user_data_user_agent,
      SAFE_CAST(last_attributed_touch_data_via_features AS STRING) AS last_attributed_touch_data_via_features,
      SAFE_CAST(last_attributed_touch_timestamp AS INT64) AS last_attributed_touch_timestamp,
      SAFE_CAST(last_attributed_touch_type AS STRING) AS last_attributed_touch_type,
      SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
      SAFE_CAST(minutes_from_last_attributed_touch_to_event AS INT64) AS minutes_from_last_attributed_touch_to_event,
      SAFE_CAST(name AS STRING) AS name,
      SAFE_CAST(origin AS STRING) AS origin,
      SAFE_CAST(original_timestamp AS TIMESTAMP) AS original_timestamp,
      SAFE_CAST(ott AS STRING) AS ott,
      SAFE_CAST(received_at AS TIMESTAMP) AS received_at,
      SAFE_CAST(reengagement_activity_attributed AS BOOLEAN) AS reengagement_activity_attributed,
      SAFE_CAST(reengagement_activity_data_country_code AS STRING) AS reengagement_activity_data_country_code,
      SAFE_CAST(reengagement_activity_event_name AS STRING) AS reengagement_activity_event_name,
      SAFE_CAST(reengagement_activity_timestamp AS INT64) AS reengagement_activity_timestamp,
      SAFE_CAST(reengagement_activity_touch_data_dollar_3p AS STRING) AS reengagement_activity_touch_data_dollar_3p,
      SAFE_CAST(reengagement_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(reengagement_activity_touch_data_tilde_advertising_partner_name AS STRING) AS reengagement_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(null AS INT64) AS seconds_from_install_to_event,
      SAFE_CAST(seconds_from_last_attributed_touch_to_event AS INT64) AS seconds_from_last_attributed_touch_to_event,
      SAFE_CAST(seconds_from_last_attributed_touch_to_store_install_begin AS INT64) AS seconds_from_last_attributed_touch_to_store_install_begin,
      SAFE_CAST(sent_at AS TIMESTAMP) AS sent_at,
      SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
      SAFE_CAST(user_data_aaid AS STRING) AS user_data_aaid,
      SAFE_CAST(user_data_android_id AS STRING) AS user_data_android_id,
      SAFE_CAST(user_data_app_version AS STRING) AS user_data_app_version,
      SAFE_CAST(user_data_brand AS STRING) AS user_data_brand,
      SAFE_CAST(user_data_build AS STRING) AS user_data_build,
      SAFE_CAST(user_data_carrier_name AS STRING) AS user_data_carrier_name,
      SAFE_CAST(user_data_cpu_type AS STRING) AS user_data_cpu_type,
      SAFE_CAST(user_data_developer_identity AS STRING) AS user_data_developer_identity,
      SAFE_CAST(user_data_disable_ad_network_callouts AS BOOLEAN) AS user_data_disable_ad_network_callouts,
      SAFE_CAST(user_data_environment AS STRING) AS user_data_environment,
      SAFE_CAST(user_data_geo_city_code AS INT64) AS user_data_geo_city_code,
      SAFE_CAST(user_data_geo_city_en AS STRING) AS user_data_geo_city_en,
      SAFE_CAST(user_data_geo_continent_code AS STRING) AS user_data_geo_continent_code,
      SAFE_CAST(user_data_geo_country_code AS STRING) AS user_data_geo_country_code,
      SAFE_CAST(user_data_geo_country_en AS STRING) AS user_data_geo_country_en,
      SAFE_CAST(user_data_geo_dma_code AS INT64) AS user_data_geo_dma_code,
      SAFE_CAST(user_data_geo_lat AS FLOAT64) AS user_data_geo_lat,
      SAFE_CAST(user_data_geo_lon AS FLOAT64) AS user_data_geo_lon,
      SAFE_CAST(user_data_geo_region_code AS STRING) AS user_data_geo_region_code,
      SAFE_CAST(user_data_geo_region_en AS STRING) AS user_data_geo_region_en,
      SAFE_CAST(null AS STRING) AS user_data_http_referrer,
      SAFE_CAST(user_data_idfa AS STRING) AS user_data_idfa,
      SAFE_CAST(user_data_idfv AS STRING) AS user_data_idfv,
      SAFE_CAST(user_data_internet_connection_type AS STRING) AS user_data_internet_connection_type,
      SAFE_CAST(user_data_ip AS STRING) AS user_data_ip,
      SAFE_CAST(user_data_language AS STRING) AS user_data_language,
      SAFE_CAST(user_data_limit_ad_tracking AS BOOLEAN) AS user_data_limit_ad_tracking,
      SAFE_CAST(user_data_model AS STRING) AS user_data_model,
      SAFE_CAST(user_data_opted_in AS BOOLEAN) AS user_data_opted_in,
      SAFE_CAST(user_data_opted_in_status AS STRING) AS user_data_opted_in_status,
      SAFE_CAST(user_data_os AS STRING) AS user_data_os,
      SAFE_CAST(user_data_os_version AS STRING) AS user_data_os_version,
      SAFE_CAST(user_data_os_version_android AS STRING) AS user_data_os_version_android,
      SAFE_CAST(user_data_past_cross_platform_ids AS STRING) AS user_data_past_cross_platform_ids,
      SAFE_CAST(user_data_platform AS STRING) AS user_data_platform,
      SAFE_CAST(user_data_private_relay AS BOOLEAN) AS user_data_private_relay,
      SAFE_CAST(user_data_prob_cross_platform_ids AS STRING) AS user_data_prob_cross_platform_ids,
      SAFE_CAST(user_data_screen_height AS INT64) AS user_data_screen_height,
      SAFE_CAST(user_data_screen_width AS INT64) AS user_data_screen_width,
      SAFE_CAST(user_data_sdk_version AS STRING) AS user_data_sdk_version,
      SAFE_CAST(user_data_user_agent AS STRING) AS user_data_user_agent,
      SAFE_CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
      SAFE_CAST(last_attributed_touch_data_always_deeplink AS STRING) AS last_attributed_touch_data_always_deeplink,
      SAFE_CAST(last_attributed_touch_data_dollar_cross_device AS BOOLEAN) AS last_attributed_touch_data_dollar_cross_device,
      SAFE_CAST(last_attributed_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS last_attributed_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS last_attributed_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(last_attributed_touch_data_fallback_url AS STRING) AS last_attributed_touch_data_fallback_url,
      SAFE_CAST(last_attributed_touch_data_gclid AS STRING) AS last_attributed_touch_data_gclid,
      SAFE_CAST(last_attributed_touch_data_placement AS STRING) AS last_attributed_touch_data_placement,
      SAFE_CAST(last_attributed_touch_data_view_through AS BOOLEAN) AS last_attributed_touch_data_view_through,
      SAFE_CAST(last_attributed_touch_data_view_time AS INT64) AS last_attributed_touch_data_view_time,
      SAFE_CAST(last_attributed_touch_data_view_timestamp AS STRING) AS last_attributed_touch_data_view_timestamp,
      SAFE_CAST(store_install_begin_timestamp AS INT64) AS store_install_begin_timestamp,
      SAFE_CAST(referrer_click_timestamp AS INT64) AS referrer_click_timestamp,
      SAFE_CAST(custom_data_opt_in AS STRING) AS custom_data_opt_in,
      SAFE_CAST(days_from_install_to_opt_in AS INT64) AS days_from_install_to_opt_in,
      SAFE_CAST(last_attributed_touch_data_domain AS STRING) AS last_attributed_touch_data_domain,
      SAFE_CAST(last_attributed_touch_data_gb AS STRING) AS last_attributed_touch_data_gb,
      SAFE_CAST(last_attributed_touch_data_matching_ttl_s AS STRING) AS last_attributed_touch_data_matching_ttl_s,
      SAFE_CAST(last_attributed_touch_data_referrer AS STRING) AS last_attributed_touch_data_referrer,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_platform_source,
      SAFE_CAST(last_attributed_touch_data_creation_source AS INT64) AS last_attributed_touch_data_creation_source,
      SAFE_CAST(last_attributed_touch_data_id AS INT64) AS last_attributed_touch_data_id,
      SAFE_CAST(last_attributed_touch_data_marketing AS BOOLEAN) AS last_attributed_touch_data_marketing,
      SAFE_CAST(last_attributed_touch_data_marketing_title AS STRING) AS last_attributed_touch_data_marketing_title,
      SAFE_CAST(last_attributed_touch_data_og_app_id AS STRING) AS last_attributed_touch_data_og_app_id,
      SAFE_CAST(last_attributed_touch_data_og_description AS STRING) AS last_attributed_touch_data_og_description,
      SAFE_CAST(last_attributed_touch_data_og_image_url AS STRING) AS last_attributed_touch_data_og_image_url,
      SAFE_CAST(last_attributed_touch_data_og_title AS STRING) AS last_attributed_touch_data_og_title,
      SAFE_CAST(last_attributed_touch_data_og_type AS STRING) AS last_attributed_touch_data_og_type,
      SAFE_CAST(last_attributed_touch_data_twitter_card AS STRING) AS last_attributed_touch_data_twitter_card,
      SAFE_CAST(last_attributed_touch_data_twitter_description AS STRING) AS last_attributed_touch_data_twitter_description,
      SAFE_CAST(last_attributed_touch_data_twitter_title AS STRING) AS last_attributed_touch_data_twitter_title,
      SAFE_CAST(last_attributed_touch_data_click_id AS STRING) AS last_attributed_touch_data_click_id,
      SAFE_CAST(last_attributed_touch_data_msclkid AS STRING) AS last_attributed_touch_data_msclkid,
      SAFE_CAST(last_attributed_touch_data_web_only AS STRING) AS last_attributed_touch_data_web_only,
      SAFE_CAST(null AS STRING) AS event_data_currency,
      SAFE_CAST(null AS STRING) AS event_data_description,
      SAFE_CAST(null AS INT64) AS event_data_exchange_rate,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue,
      SAFE_CAST(null AS INT64) AS event_data_revenue_in_local_currency,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue_in_usd,
      SAFE_CAST(null AS STRING) AS event_data_transaction_id,
      SAFE_CAST(null AS STRING) AS user_data_browser
      FROM
      branch_io_v2.install
      union all
      SELECT
      SAFE_CAST(_id AS STRING) AS _id,
      SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
      SAFE_CAST(attributed AS STRING) AS attributed,
      SAFE_CAST(content_items AS STRING) AS content_items,
      SAFE_CAST(context_library_name AS STRING) AS context_library_name,
      SAFE_CAST(context_library_version AS STRING) AS context_library_version,
      SAFE_CAST(cross_device_ott AS STRING) AS cross_device_ott,
      SAFE_CAST(custom_data_gateway AS STRING) AS custom_data_gateway,
      SAFE_CAST(custom_data_segment_anonymous_id AS STRING) AS custom_data_segment_anonymous_id,
      SAFE_CAST(custom_data_skan_time_window AS STRING) AS custom_data_skan_time_window,
      SAFE_CAST(days_from_last_attributed_touch_to_event AS INT64) AS days_from_last_attributed_touch_to_event,
      SAFE_CAST(deep_linked AS STRING) AS deep_linked,
      SAFE_CAST(event AS STRING) AS event,
      SAFE_CAST(event_days_from_timestamp AS INT64) AS event_days_from_timestamp,
      SAFE_CAST(event_text AS STRING) AS event_text,
      SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
      SAFE_CAST(existing_user AS STRING) AS existing_user,
      SAFE_CAST(first_event_for_user AS STRING) AS first_event_for_user,
      SAFE_CAST(hours_from_last_attributed_touch_to_event AS INT64) AS hours_from_last_attributed_touch_to_event,
      SAFE_CAST(id AS STRING) AS id,
      SAFE_CAST(install_activity_attributed AS BOOLEAN) AS install_activity_attributed,
      SAFE_CAST(install_activity_data_country_code AS STRING) AS install_activity_data_country_code,
      SAFE_CAST(install_activity_event_name AS STRING) AS install_activity_event_name,
      SAFE_CAST(install_activity_timestamp AS INT64) AS install_activity_timestamp,
      SAFE_CAST(install_activity_touch_data_dollar_3p AS STRING) AS install_activity_touch_data_dollar_3p,
      SAFE_CAST(install_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS install_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(install_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS install_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(install_activity_touch_data_tilde_advertising_partner_name AS STRING) AS install_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_3p AS STRING) AS last_attributed_touch_data_3p,
      SAFE_CAST(last_attributed_touch_data_ad_id AS STRING) AS last_attributed_touch_data_ad_id,
      SAFE_CAST(last_attributed_touch_data_ad_name AS STRING) AS last_attributed_touch_data_ad_name,
      SAFE_CAST(last_attributed_touch_data_ad_objective_name AS STRING) AS last_attributed_touch_data_ad_objective_name,
      SAFE_CAST(last_attributed_touch_data_ad_set_id AS STRING) AS last_attributed_touch_data_ad_set_id,
      SAFE_CAST(last_attributed_touch_data_ad_set_name AS STRING) AS last_attributed_touch_data_ad_set_name,
      SAFE_CAST(last_attributed_touch_data_advertising_account_id AS STRING) AS last_attributed_touch_data_advertising_account_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_id AS STRING) AS last_attributed_touch_data_advertising_partner_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_name AS STRING) AS last_attributed_touch_data_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_android_passive_deepview AS STRING) AS last_attributed_touch_data_android_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_android_url AS STRING) AS last_attributed_touch_data_android_url,
      SAFE_CAST(last_attributed_touch_data_api_open_click AS STRING) AS last_attributed_touch_data_api_open_click,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution AS BOOLEAN) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date AS TIMESTAMP) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id,
      SAFE_CAST(last_attributed_touch_data_branch_ad_format AS STRING) AS last_attributed_touch_data_branch_ad_format,
      SAFE_CAST(last_attributed_touch_data_campaign AS STRING) AS last_attributed_touch_data_campaign,
      SAFE_CAST(last_attributed_touch_data_campaign_id AS STRING) AS last_attributed_touch_data_campaign_id,
      SAFE_CAST(last_attributed_touch_data_campaign_type AS STRING) AS last_attributed_touch_data_campaign_type,
      SAFE_CAST(last_attributed_touch_data_canonical_url AS STRING) AS last_attributed_touch_data_canonical_url,
      SAFE_CAST(last_attributed_touch_data_channel AS STRING) AS last_attributed_touch_data_channel,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser_version AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser_version,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_is_mobile AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_is_mobile,
      SAFE_CAST(last_attributed_touch_data_click_timestamp AS INT64) AS last_attributed_touch_data_click_timestamp,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_collection,
      SAFE_CAST(last_attributed_touch_data_conversion_type AS STRING) AS last_attributed_touch_data_conversion_type,
      SAFE_CAST(last_attributed_touch_data_country_or_region AS STRING) AS last_attributed_touch_data_country_or_region,
      SAFE_CAST(last_attributed_touch_data_creative_id AS STRING) AS last_attributed_touch_data_creative_id,
      SAFE_CAST(last_attributed_touch_data_creative_name AS STRING) AS last_attributed_touch_data_creative_name,
      SAFE_CAST(last_attributed_touch_data_desktop_url AS STRING) AS last_attributed_touch_data_desktop_url,
      SAFE_CAST(last_attributed_touch_data_device_brand_model AS STRING) AS last_attributed_touch_data_device_brand_model,
      SAFE_CAST(last_attributed_touch_data_device_brand_name AS STRING) AS last_attributed_touch_data_device_brand_name,
      SAFE_CAST(last_attributed_touch_data_device_os AS STRING) AS last_attributed_touch_data_device_os,
      SAFE_CAST(last_attributed_touch_data_device_os_version AS STRING) AS last_attributed_touch_data_device_os_version,
      SAFE_CAST(last_attributed_touch_data_fb_data_terms_not_signed AS STRING) AS last_attributed_touch_data_fb_data_terms_not_signed,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_fbclid,
      SAFE_CAST(last_attributed_touch_data_feature AS STRING) AS last_attributed_touch_data_feature,
      SAFE_CAST(last_attributed_touch_data_geo_country_code AS STRING) AS last_attributed_touch_data_geo_country_code,
      SAFE_CAST(last_attributed_touch_data_ios_passive_deepview AS STRING) AS last_attributed_touch_data_ios_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_ios_url AS STRING) AS last_attributed_touch_data_ios_url,
      SAFE_CAST(last_attributed_touch_data_is_mobile_data_terms_signed AS STRING) AS last_attributed_touch_data_is_mobile_data_terms_signed,
      SAFE_CAST(last_attributed_touch_data_keyword AS STRING) AS last_attributed_touch_data_keyword,
      SAFE_CAST(last_attributed_touch_data_keyword_id AS STRING) AS last_attributed_touch_data_keyword_id,
      SAFE_CAST(last_attributed_touch_data_keyword_match_type AS STRING) AS last_attributed_touch_data_keyword_match_type,
      SAFE_CAST(last_attributed_touch_data_link_title AS STRING) AS last_attributed_touch_data_link_title,
      SAFE_CAST(last_attributed_touch_data_link_type AS STRING) AS last_attributed_touch_data_link_type,
      SAFE_CAST(last_attributed_touch_data_one_time_use AS STRING) AS last_attributed_touch_data_one_time_use,
      SAFE_CAST(last_attributed_touch_data_organic_search_url AS STRING) AS last_attributed_touch_data_organic_search_url,
      SAFE_CAST(last_attributed_touch_data_secondary_ad_format AS STRING) AS last_attributed_touch_data_secondary_ad_format,
      SAFE_CAST(last_attributed_touch_data_secondary_publisher AS STRING) AS last_attributed_touch_data_secondary_publisher,
      SAFE_CAST(last_attributed_touch_data_tags AS STRING) AS last_attributed_touch_data_tags,
      SAFE_CAST(last_attributed_touch_data_touch_id AS STRING) AS last_attributed_touch_data_touch_id,
      SAFE_CAST(last_attributed_touch_data_touch_subtype AS STRING) AS last_attributed_touch_data_touch_subtype,
      SAFE_CAST(last_attributed_touch_data_tune_publisher_name AS STRING) AS last_attributed_touch_data_tune_publisher_name,
      SAFE_CAST(last_attributed_touch_data_url AS STRING) AS last_attributed_touch_data_url,
      SAFE_CAST(last_attributed_touch_data_user_data_ip AS STRING) AS last_attributed_touch_data_user_data_ip,
      SAFE_CAST(last_attributed_touch_data_user_data_user_agent AS STRING) AS last_attributed_touch_data_user_data_user_agent,
      SAFE_CAST(last_attributed_touch_data_via_features AS STRING) AS last_attributed_touch_data_via_features,
      SAFE_CAST(last_attributed_touch_timestamp AS INT64) AS last_attributed_touch_timestamp,
      SAFE_CAST(last_attributed_touch_type AS STRING) AS last_attributed_touch_type,
      SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
      SAFE_CAST(minutes_from_last_attributed_touch_to_event AS INT64) AS minutes_from_last_attributed_touch_to_event,
      SAFE_CAST(name AS STRING) AS name,
      SAFE_CAST(origin AS STRING) AS origin,
      SAFE_CAST(original_timestamp AS TIMESTAMP) AS original_timestamp,
      SAFE_CAST(ott AS STRING) AS ott,
      SAFE_CAST(received_at AS TIMESTAMP) AS received_at,
      SAFE_CAST(reengagement_activity_attributed AS BOOLEAN) AS reengagement_activity_attributed,
      SAFE_CAST(reengagement_activity_data_country_code AS STRING) AS reengagement_activity_data_country_code,
      SAFE_CAST(reengagement_activity_event_name AS STRING) AS reengagement_activity_event_name,
      SAFE_CAST(reengagement_activity_timestamp AS INT64) AS reengagement_activity_timestamp,
      SAFE_CAST(reengagement_activity_touch_data_dollar_3p AS STRING) AS reengagement_activity_touch_data_dollar_3p,
      SAFE_CAST(reengagement_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(reengagement_activity_touch_data_tilde_advertising_partner_name AS STRING) AS reengagement_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(seconds_from_install_to_event AS INT64) AS seconds_from_install_to_event,
      SAFE_CAST(seconds_from_last_attributed_touch_to_event AS INT64) AS seconds_from_last_attributed_touch_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_store_install_begin,
      SAFE_CAST(sent_at AS TIMESTAMP) AS sent_at,
      SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
      SAFE_CAST(user_data_aaid AS STRING) AS user_data_aaid,
      SAFE_CAST(user_data_android_id AS STRING) AS user_data_android_id,
      SAFE_CAST(user_data_app_version AS STRING) AS user_data_app_version,
      SAFE_CAST(user_data_brand AS STRING) AS user_data_brand,
      SAFE_CAST(user_data_build AS STRING) AS user_data_build,
      SAFE_CAST(user_data_carrier_name AS STRING) AS user_data_carrier_name,
      SAFE_CAST(user_data_cpu_type AS STRING) AS user_data_cpu_type,
      SAFE_CAST(user_data_developer_identity AS STRING) AS user_data_developer_identity,
      SAFE_CAST(user_data_disable_ad_network_callouts AS BOOLEAN) AS user_data_disable_ad_network_callouts,
      SAFE_CAST(user_data_environment AS STRING) AS user_data_environment,
      SAFE_CAST(user_data_geo_city_code AS INT64) AS user_data_geo_city_code,
      SAFE_CAST(user_data_geo_city_en AS STRING) AS user_data_geo_city_en,
      SAFE_CAST(user_data_geo_continent_code AS STRING) AS user_data_geo_continent_code,
      SAFE_CAST(user_data_geo_country_code AS STRING) AS user_data_geo_country_code,
      SAFE_CAST(user_data_geo_country_en AS STRING) AS user_data_geo_country_en,
      SAFE_CAST(user_data_geo_dma_code AS INT64) AS user_data_geo_dma_code,
      SAFE_CAST(user_data_geo_lat AS FLOAT64) AS user_data_geo_lat,
      SAFE_CAST(user_data_geo_lon AS FLOAT64) AS user_data_geo_lon,
      SAFE_CAST(user_data_geo_region_code AS STRING) AS user_data_geo_region_code,
      SAFE_CAST(user_data_geo_region_en AS STRING) AS user_data_geo_region_en,
      SAFE_CAST(null AS STRING) AS user_data_http_referrer,
      SAFE_CAST(user_data_idfa AS STRING) AS user_data_idfa,
      SAFE_CAST(user_data_idfv AS STRING) AS user_data_idfv,
      SAFE_CAST(user_data_internet_connection_type AS STRING) AS user_data_internet_connection_type,
      SAFE_CAST(user_data_ip AS STRING) AS user_data_ip,
      SAFE_CAST(user_data_language AS STRING) AS user_data_language,
      SAFE_CAST(user_data_limit_ad_tracking AS BOOLEAN) AS user_data_limit_ad_tracking,
      SAFE_CAST(user_data_model AS STRING) AS user_data_model,
      SAFE_CAST(user_data_opted_in AS BOOLEAN) AS user_data_opted_in,
      SAFE_CAST(user_data_opted_in_status AS STRING) AS user_data_opted_in_status,
      SAFE_CAST(user_data_os AS STRING) AS user_data_os,
      SAFE_CAST(user_data_os_version AS STRING) AS user_data_os_version,
      SAFE_CAST(user_data_os_version_android AS STRING) AS user_data_os_version_android,
      SAFE_CAST(user_data_past_cross_platform_ids AS STRING) AS user_data_past_cross_platform_ids,
      SAFE_CAST(user_data_platform AS STRING) AS user_data_platform,
      SAFE_CAST(user_data_private_relay AS BOOLEAN) AS user_data_private_relay,
      SAFE_CAST(user_data_prob_cross_platform_ids AS STRING) AS user_data_prob_cross_platform_ids,
      SAFE_CAST(user_data_screen_height AS INT64) AS user_data_screen_height,
      SAFE_CAST(user_data_screen_width AS INT64) AS user_data_screen_width,
      SAFE_CAST(user_data_sdk_version AS STRING) AS user_data_sdk_version,
      SAFE_CAST(user_data_user_agent AS STRING) AS user_data_user_agent,
      SAFE_CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
      SAFE_CAST(last_attributed_touch_data_always_deeplink AS STRING) AS last_attributed_touch_data_always_deeplink,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_cross_device,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(last_attributed_touch_data_fallback_url AS STRING) AS last_attributed_touch_data_fallback_url,
      SAFE_CAST(last_attributed_touch_data_gclid AS STRING) AS last_attributed_touch_data_gclid,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_placement,
      SAFE_CAST(last_attributed_touch_data_view_through AS BOOLEAN) AS last_attributed_touch_data_view_through,
      SAFE_CAST(null AS INT64) AS last_attributed_touch_data_view_time,
      SAFE_CAST(last_attributed_touch_data_view_timestamp AS STRING) AS last_attributed_touch_data_view_timestamp,
      SAFE_CAST(store_install_begin_timestamp AS INT64) AS store_install_begin_timestamp,
      SAFE_CAST(referrer_click_timestamp AS INT64) AS referrer_click_timestamp,
      SAFE_CAST(custom_data_opt_in AS STRING) AS custom_data_opt_in,
      SAFE_CAST(null AS INT64) AS days_from_install_to_opt_in,
      SAFE_CAST(last_attributed_touch_data_domain AS STRING) AS last_attributed_touch_data_domain,
      SAFE_CAST(last_attributed_touch_data_gb AS STRING) AS last_attributed_touch_data_gb,
      SAFE_CAST(last_attributed_touch_data_matching_ttl_s AS STRING) AS last_attributed_touch_data_matching_ttl_s,
      SAFE_CAST(last_attributed_touch_data_referrer AS STRING) AS last_attributed_touch_data_referrer,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_platform_source,
      SAFE_CAST(null AS INT64) AS last_attributed_touch_data_creation_source,
      SAFE_CAST(null AS INT64) AS last_attributed_touch_data_id,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_marketing,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_marketing_title,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_og_app_id,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_og_description,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_og_image_url,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_og_title,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_og_type,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_twitter_card,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_twitter_description,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_twitter_title,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_click_id,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_msclkid,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_web_only,
      SAFE_CAST(null AS STRING) AS event_data_currency,
      SAFE_CAST(null AS STRING) AS event_data_description,
      SAFE_CAST(null AS INT64) AS event_data_exchange_rate,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue,
      SAFE_CAST(null AS INT64) AS event_data_revenue_in_local_currency,
      SAFE_CAST(null AS FLOAT64) AS event_data_revenue_in_usd,
      SAFE_CAST(null AS STRING) AS event_data_transaction_id,
      SAFE_CAST(null AS STRING) AS user_data_browser
      FROM
      branch_io_v2.reinstall
      union all
      SELECT
      SAFE_CAST(_id AS STRING) AS _id,
      SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
      SAFE_CAST(attributed AS STRING) AS attributed,
      SAFE_CAST(content_items AS STRING) AS content_items,
      SAFE_CAST(context_library_name AS STRING) AS context_library_name,
      SAFE_CAST(context_library_version AS STRING) AS context_library_version,
      SAFE_CAST(cross_device_ott AS STRING) AS cross_device_ott,
      SAFE_CAST(null AS STRING) AS custom_data_gateway,
      SAFE_CAST(custom_data_segment_anonymous_id AS STRING) AS custom_data_segment_anonymous_id,
      SAFE_CAST(custom_data_skan_time_window AS STRING) AS custom_data_skan_time_window,
      SAFE_CAST(days_from_last_attributed_touch_to_event AS INT64) AS days_from_last_attributed_touch_to_event,
      SAFE_CAST(deep_linked AS STRING) AS deep_linked,
      SAFE_CAST(event AS STRING) AS event,
      SAFE_CAST(event_days_from_timestamp AS INT64) AS event_days_from_timestamp,
      SAFE_CAST(event_text AS STRING) AS event_text,
      SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
      SAFE_CAST(existing_user AS STRING) AS existing_user,
      SAFE_CAST(first_event_for_user AS STRING) AS first_event_for_user,
      SAFE_CAST(null AS INT64) AS hours_from_last_attributed_touch_to_event,
      SAFE_CAST(id AS STRING) AS id,
      SAFE_CAST(install_activity_attributed AS BOOLEAN) AS install_activity_attributed,
      SAFE_CAST(install_activity_data_country_code AS STRING) AS install_activity_data_country_code,
      SAFE_CAST(install_activity_event_name AS STRING) AS install_activity_event_name,
      SAFE_CAST(install_activity_timestamp AS INT64) AS install_activity_timestamp,
      SAFE_CAST(install_activity_touch_data_dollar_3p AS STRING) AS install_activity_touch_data_dollar_3p,
      SAFE_CAST(install_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS install_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(install_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS install_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(install_activity_touch_data_tilde_advertising_partner_name AS STRING) AS install_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_3p AS STRING) AS last_attributed_touch_data_3p,
      SAFE_CAST(last_attributed_touch_data_ad_id AS STRING) AS last_attributed_touch_data_ad_id,
      SAFE_CAST(last_attributed_touch_data_ad_name AS STRING) AS last_attributed_touch_data_ad_name,
      SAFE_CAST(last_attributed_touch_data_ad_objective_name AS STRING) AS last_attributed_touch_data_ad_objective_name,
      SAFE_CAST(last_attributed_touch_data_ad_set_id AS STRING) AS last_attributed_touch_data_ad_set_id,
      SAFE_CAST(last_attributed_touch_data_ad_set_name AS STRING) AS last_attributed_touch_data_ad_set_name,
      SAFE_CAST(last_attributed_touch_data_advertising_account_id AS STRING) AS last_attributed_touch_data_advertising_account_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_id AS STRING) AS last_attributed_touch_data_advertising_partner_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_name AS STRING) AS last_attributed_touch_data_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_android_passive_deepview AS STRING) AS last_attributed_touch_data_android_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_android_url AS STRING) AS last_attributed_touch_data_android_url,
      SAFE_CAST(last_attributed_touch_data_api_open_click AS STRING) AS last_attributed_touch_data_api_open_click,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution AS BOOLEAN) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date AS TIMESTAMP) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id,
      SAFE_CAST(last_attributed_touch_data_branch_ad_format AS STRING) AS last_attributed_touch_data_branch_ad_format,
      SAFE_CAST(last_attributed_touch_data_campaign AS STRING) AS last_attributed_touch_data_campaign,
      SAFE_CAST(last_attributed_touch_data_campaign_id AS STRING) AS last_attributed_touch_data_campaign_id,
      SAFE_CAST(last_attributed_touch_data_campaign_type AS STRING) AS last_attributed_touch_data_campaign_type,
      SAFE_CAST(last_attributed_touch_data_canonical_url AS STRING) AS last_attributed_touch_data_canonical_url,
      SAFE_CAST(last_attributed_touch_data_channel AS STRING) AS last_attributed_touch_data_channel,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser_version AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser_version,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_is_mobile AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_is_mobile,
      SAFE_CAST(last_attributed_touch_data_click_timestamp AS INT64) AS last_attributed_touch_data_click_timestamp,
      SAFE_CAST(last_attributed_touch_data_collection AS STRING) AS last_attributed_touch_data_collection,
      SAFE_CAST(last_attributed_touch_data_conversion_type AS STRING) AS last_attributed_touch_data_conversion_type,
      SAFE_CAST(last_attributed_touch_data_country_or_region AS STRING) AS last_attributed_touch_data_country_or_region,
      SAFE_CAST(last_attributed_touch_data_creative_id AS STRING) AS last_attributed_touch_data_creative_id,
      SAFE_CAST(last_attributed_touch_data_creative_name AS STRING) AS last_attributed_touch_data_creative_name,
      SAFE_CAST(last_attributed_touch_data_desktop_url AS STRING) AS last_attributed_touch_data_desktop_url,
      SAFE_CAST(last_attributed_touch_data_device_brand_model AS STRING) AS last_attributed_touch_data_device_brand_model,
      SAFE_CAST(last_attributed_touch_data_device_brand_name AS STRING) AS last_attributed_touch_data_device_brand_name,
      SAFE_CAST(last_attributed_touch_data_device_os AS STRING) AS last_attributed_touch_data_device_os,
      SAFE_CAST(last_attributed_touch_data_device_os_version AS STRING) AS last_attributed_touch_data_device_os_version,
      SAFE_CAST(last_attributed_touch_data_fb_data_terms_not_signed AS STRING) AS last_attributed_touch_data_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_fbclid AS STRING) AS last_attributed_touch_data_fbclid,
      SAFE_CAST(last_attributed_touch_data_feature AS STRING) AS last_attributed_touch_data_feature,
      SAFE_CAST(last_attributed_touch_data_geo_country_code AS STRING) AS last_attributed_touch_data_geo_country_code,
      SAFE_CAST(last_attributed_touch_data_ios_passive_deepview AS STRING) AS last_attributed_touch_data_ios_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_ios_url AS STRING) AS last_attributed_touch_data_ios_url,
      SAFE_CAST(last_attributed_touch_data_is_mobile_data_terms_signed AS STRING) AS last_attributed_touch_data_is_mobile_data_terms_signed,
      SAFE_CAST(last_attributed_touch_data_keyword AS STRING) AS last_attributed_touch_data_keyword,
      SAFE_CAST(last_attributed_touch_data_keyword_id AS STRING) AS last_attributed_touch_data_keyword_id,
      SAFE_CAST(last_attributed_touch_data_keyword_match_type AS STRING) AS last_attributed_touch_data_keyword_match_type,
      SAFE_CAST(last_attributed_touch_data_link_title AS STRING) AS last_attributed_touch_data_link_title,
      SAFE_CAST(last_attributed_touch_data_link_type AS STRING) AS last_attributed_touch_data_link_type,
      SAFE_CAST(last_attributed_touch_data_one_time_use AS STRING) AS last_attributed_touch_data_one_time_use,
      SAFE_CAST(last_attributed_touch_data_organic_search_url AS STRING) AS last_attributed_touch_data_organic_search_url,
      SAFE_CAST(last_attributed_touch_data_secondary_ad_format AS STRING) AS last_attributed_touch_data_secondary_ad_format,
      SAFE_CAST(last_attributed_touch_data_secondary_publisher AS STRING) AS last_attributed_touch_data_secondary_publisher,
      SAFE_CAST(last_attributed_touch_data_tags AS STRING) AS last_attributed_touch_data_tags,
      SAFE_CAST(last_attributed_touch_data_touch_id AS STRING) AS last_attributed_touch_data_touch_id,
      SAFE_CAST(last_attributed_touch_data_touch_subtype AS STRING) AS last_attributed_touch_data_touch_subtype,
      SAFE_CAST(last_attributed_touch_data_tune_publisher_name AS STRING) AS last_attributed_touch_data_tune_publisher_name,
      SAFE_CAST(last_attributed_touch_data_url AS STRING) AS last_attributed_touch_data_url,
      SAFE_CAST(last_attributed_touch_data_user_data_ip AS STRING) AS last_attributed_touch_data_user_data_ip,
      SAFE_CAST(last_attributed_touch_data_user_data_user_agent AS STRING) AS last_attributed_touch_data_user_data_user_agent,
      SAFE_CAST(last_attributed_touch_data_via_features AS STRING) AS last_attributed_touch_data_via_features,
      SAFE_CAST(last_attributed_touch_timestamp AS INT64) AS last_attributed_touch_timestamp,
      SAFE_CAST(last_attributed_touch_type AS STRING) AS last_attributed_touch_type,
      SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
      SAFE_CAST(null AS INT64) AS minutes_from_last_attributed_touch_to_event,
      SAFE_CAST(name AS STRING) AS name,
      SAFE_CAST(origin AS STRING) AS origin,
      SAFE_CAST(original_timestamp AS TIMESTAMP) AS original_timestamp,
      SAFE_CAST(ott AS STRING) AS ott,
      SAFE_CAST(received_at AS TIMESTAMP) AS received_at,
      SAFE_CAST(reengagement_activity_attributed AS BOOLEAN) AS reengagement_activity_attributed,
      SAFE_CAST(reengagement_activity_data_country_code AS STRING) AS reengagement_activity_data_country_code,
      SAFE_CAST(reengagement_activity_event_name AS STRING) AS reengagement_activity_event_name,
      SAFE_CAST(reengagement_activity_timestamp AS INT64) AS reengagement_activity_timestamp,
      SAFE_CAST(reengagement_activity_touch_data_dollar_3p AS STRING) AS reengagement_activity_touch_data_dollar_3p,
      SAFE_CAST(reengagement_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(reengagement_activity_touch_data_tilde_advertising_partner_name AS STRING) AS reengagement_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(seconds_from_install_to_event AS INT64) AS seconds_from_install_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_store_install_begin,
      SAFE_CAST(sent_at AS TIMESTAMP) AS sent_at,
      SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
      SAFE_CAST(user_data_aaid AS STRING) AS user_data_aaid,
      SAFE_CAST(user_data_android_id AS STRING) AS user_data_android_id,
      SAFE_CAST(user_data_app_version AS STRING) AS user_data_app_version,
      SAFE_CAST(user_data_brand AS STRING) AS user_data_brand,
      SAFE_CAST(user_data_build AS STRING) AS user_data_build,
      SAFE_CAST(user_data_carrier_name AS STRING) AS user_data_carrier_name,
      SAFE_CAST(user_data_cpu_type AS STRING) AS user_data_cpu_type,
      SAFE_CAST(user_data_developer_identity AS STRING) AS user_data_developer_identity,
      SAFE_CAST(user_data_disable_ad_network_callouts AS BOOLEAN) AS user_data_disable_ad_network_callouts,
      SAFE_CAST(user_data_environment AS STRING) AS user_data_environment,
      SAFE_CAST(user_data_geo_city_code AS INT64) AS user_data_geo_city_code,
      SAFE_CAST(user_data_geo_city_en AS STRING) AS user_data_geo_city_en,
      SAFE_CAST(user_data_geo_continent_code AS STRING) AS user_data_geo_continent_code,
      SAFE_CAST(user_data_geo_country_code AS STRING) AS user_data_geo_country_code,
      SAFE_CAST(user_data_geo_country_en AS STRING) AS user_data_geo_country_en,
      SAFE_CAST(user_data_geo_dma_code AS INT64) AS user_data_geo_dma_code,
      SAFE_CAST(user_data_geo_lat AS FLOAT64) AS user_data_geo_lat,
      SAFE_CAST(user_data_geo_lon AS FLOAT64) AS user_data_geo_lon,
      SAFE_CAST(user_data_geo_region_code AS STRING) AS user_data_geo_region_code,
      SAFE_CAST(user_data_geo_region_en AS STRING) AS user_data_geo_region_en,
      SAFE_CAST(null AS STRING) AS user_data_http_referrer,
      SAFE_CAST(user_data_idfa AS STRING) AS user_data_idfa,
      SAFE_CAST(user_data_idfv AS STRING) AS user_data_idfv,
      SAFE_CAST(user_data_internet_connection_type AS STRING) AS user_data_internet_connection_type,
      SAFE_CAST(user_data_ip AS STRING) AS user_data_ip,
      SAFE_CAST(user_data_language AS STRING) AS user_data_language,
      SAFE_CAST(user_data_limit_ad_tracking AS BOOLEAN) AS user_data_limit_ad_tracking,
      SAFE_CAST(user_data_model AS STRING) AS user_data_model,
      SAFE_CAST(user_data_opted_in AS BOOLEAN) AS user_data_opted_in,
      SAFE_CAST(user_data_opted_in_status AS STRING) AS user_data_opted_in_status,
      SAFE_CAST(user_data_os AS STRING) AS user_data_os,
      SAFE_CAST(user_data_os_version AS STRING) AS user_data_os_version,
      SAFE_CAST(user_data_os_version_android AS STRING) AS user_data_os_version_android,
      SAFE_CAST(user_data_past_cross_platform_ids AS STRING) AS user_data_past_cross_platform_ids,
      SAFE_CAST(user_data_platform AS STRING) AS user_data_platform,
      SAFE_CAST(user_data_private_relay AS BOOLEAN) AS user_data_private_relay,
      SAFE_CAST(user_data_prob_cross_platform_ids AS STRING) AS user_data_prob_cross_platform_ids,
      SAFE_CAST(user_data_screen_height AS INT64) AS user_data_screen_height,
      SAFE_CAST(user_data_screen_width AS INT64) AS user_data_screen_width,
      SAFE_CAST(user_data_sdk_version AS STRING) AS user_data_sdk_version,
      SAFE_CAST(user_data_user_agent AS STRING) AS user_data_user_agent,
      SAFE_CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
      SAFE_CAST(last_attributed_touch_data_always_deeplink AS STRING) AS last_attributed_touch_data_always_deeplink,
      SAFE_CAST(last_attributed_touch_data_dollar_cross_device AS BOOLEAN) AS last_attributed_touch_data_dollar_cross_device,
      SAFE_CAST(last_attributed_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS last_attributed_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(last_attributed_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS last_attributed_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(last_attributed_touch_data_fallback_url AS STRING) AS last_attributed_touch_data_fallback_url,
      SAFE_CAST(last_attributed_touch_data_gclid AS STRING) AS last_attributed_touch_data_gclid,
      SAFE_CAST(last_attributed_touch_data_placement AS STRING) AS last_attributed_touch_data_placement,
      SAFE_CAST(last_attributed_touch_data_view_through AS BOOLEAN) AS last_attributed_touch_data_view_through,
      SAFE_CAST(last_attributed_touch_data_view_time AS INT64) AS last_attributed_touch_data_view_time,
      SAFE_CAST(last_attributed_touch_data_view_timestamp AS STRING) AS last_attributed_touch_data_view_timestamp,
      SAFE_CAST(null AS INT64) AS store_install_begin_timestamp,
      SAFE_CAST(null AS INT64) AS referrer_click_timestamp,
      SAFE_CAST(null AS STRING) AS custom_data_opt_in,
      SAFE_CAST(null AS INT64) AS days_from_install_to_opt_in,
      SAFE_CAST(last_attributed_touch_data_domain AS STRING) AS last_attributed_touch_data_domain,
      SAFE_CAST(last_attributed_touch_data_gb AS STRING) AS last_attributed_touch_data_gb,
      SAFE_CAST(last_attributed_touch_data_matching_ttl_s AS STRING) AS last_attributed_touch_data_matching_ttl_s,
      SAFE_CAST(last_attributed_touch_data_referrer AS STRING) AS last_attributed_touch_data_referrer,
      SAFE_CAST(last_attributed_touch_data_platform_source AS STRING) AS last_attributed_touch_data_platform_source,
      SAFE_CAST(last_attributed_touch_data_creation_source AS INT64) AS last_attributed_touch_data_creation_source,
      SAFE_CAST(last_attributed_touch_data_id AS INT64) AS last_attributed_touch_data_id,
      SAFE_CAST(last_attributed_touch_data_marketing AS BOOLEAN) AS last_attributed_touch_data_marketing,
      SAFE_CAST(last_attributed_touch_data_marketing_title AS STRING) AS last_attributed_touch_data_marketing_title,
      SAFE_CAST(last_attributed_touch_data_og_app_id AS STRING) AS last_attributed_touch_data_og_app_id,
      SAFE_CAST(last_attributed_touch_data_og_description AS STRING) AS last_attributed_touch_data_og_description,
      SAFE_CAST(last_attributed_touch_data_og_image_url AS STRING) AS last_attributed_touch_data_og_image_url,
      SAFE_CAST(last_attributed_touch_data_og_title AS STRING) AS last_attributed_touch_data_og_title,
      SAFE_CAST(last_attributed_touch_data_og_type AS STRING) AS last_attributed_touch_data_og_type,
      SAFE_CAST(last_attributed_touch_data_twitter_card AS STRING) AS last_attributed_touch_data_twitter_card,
      SAFE_CAST(last_attributed_touch_data_twitter_description AS STRING) AS last_attributed_touch_data_twitter_description,
      SAFE_CAST(last_attributed_touch_data_twitter_title AS STRING) AS last_attributed_touch_data_twitter_title,
      SAFE_CAST(last_attributed_touch_data_click_id AS STRING) AS last_attributed_touch_data_click_id,
      SAFE_CAST(last_attributed_touch_data_msclkid AS STRING) AS last_attributed_touch_data_msclkid,
      SAFE_CAST(last_attributed_touch_data_web_only AS STRING) AS last_attributed_touch_data_web_only,
      SAFE_CAST(event_data_currency AS STRING) AS event_data_currency,
      SAFE_CAST(event_data_description AS STRING) AS event_data_description,
      SAFE_CAST(event_data_exchange_rate AS INT64) AS event_data_exchange_rate,
      SAFE_CAST(event_data_revenue AS FLOAT64) AS event_data_revenue,
      SAFE_CAST(event_data_revenue_in_local_currency AS INT64) AS event_data_revenue_in_local_currency,
      SAFE_CAST(event_data_revenue_in_usd AS FLOAT64) AS event_data_revenue_in_usd,
      SAFE_CAST(event_data_transaction_id AS STRING) AS event_data_transaction_id,
      SAFE_CAST(user_data_browser AS STRING) AS user_data_browser
      FROM
      branch_io_v2.initiate_purchase
      union all
      SELECT
      SAFE_CAST(_id AS STRING) AS _id,
      SAFE_CAST(anonymous_id AS STRING) AS anonymous_id,
      SAFE_CAST(attributed AS STRING) AS attributed,
      SAFE_CAST(content_items AS STRING) AS content_items,
      SAFE_CAST(context_library_name AS STRING) AS context_library_name,
      SAFE_CAST(context_library_version AS STRING) AS context_library_version,
      SAFE_CAST(cross_device_ott AS STRING) AS cross_device_ott,
      SAFE_CAST(null AS STRING) AS custom_data_gateway,
      SAFE_CAST(custom_data_segment_anonymous_id AS STRING) AS custom_data_segment_anonymous_id,
      SAFE_CAST(custom_data_skan_time_window AS STRING) AS custom_data_skan_time_window,
      SAFE_CAST(days_from_last_attributed_touch_to_event AS INT64) AS days_from_last_attributed_touch_to_event,
      SAFE_CAST(deep_linked AS STRING) AS deep_linked,
      SAFE_CAST(event AS STRING) AS event,
      SAFE_CAST(event_days_from_timestamp AS INT64) AS event_days_from_timestamp,
      SAFE_CAST(event_text AS STRING) AS event_text,
      SAFE_CAST(event_timestamp AS INT64) AS event_timestamp,
      SAFE_CAST(existing_user AS STRING) AS existing_user,
      SAFE_CAST(first_event_for_user AS STRING) AS first_event_for_user,
      SAFE_CAST(null AS INT64) AS hours_from_last_attributed_touch_to_event,
      SAFE_CAST(id AS STRING) AS id,
      SAFE_CAST(install_activity_attributed AS BOOLEAN) AS install_activity_attributed,
      SAFE_CAST(install_activity_data_country_code AS STRING) AS install_activity_data_country_code,
      SAFE_CAST(install_activity_event_name AS STRING) AS install_activity_event_name,
      SAFE_CAST(install_activity_timestamp AS INT64) AS install_activity_timestamp,
      SAFE_CAST(install_activity_touch_data_dollar_3p AS STRING) AS install_activity_touch_data_dollar_3p,
      SAFE_CAST(install_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS install_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(install_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS install_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(install_activity_touch_data_tilde_advertising_partner_name AS STRING) AS install_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_3p AS STRING) AS last_attributed_touch_data_3p,
      SAFE_CAST(last_attributed_touch_data_ad_id AS STRING) AS last_attributed_touch_data_ad_id,
      SAFE_CAST(last_attributed_touch_data_ad_name AS STRING) AS last_attributed_touch_data_ad_name,
      SAFE_CAST(last_attributed_touch_data_ad_objective_name AS STRING) AS last_attributed_touch_data_ad_objective_name,
      SAFE_CAST(last_attributed_touch_data_ad_set_id AS STRING) AS last_attributed_touch_data_ad_set_id,
      SAFE_CAST(last_attributed_touch_data_ad_set_name AS STRING) AS last_attributed_touch_data_ad_set_name,
      SAFE_CAST(last_attributed_touch_data_advertising_account_id AS STRING) AS last_attributed_touch_data_advertising_account_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_id AS STRING) AS last_attributed_touch_data_advertising_partner_id,
      SAFE_CAST(last_attributed_touch_data_advertising_partner_name AS STRING) AS last_attributed_touch_data_advertising_partner_name,
      SAFE_CAST(last_attributed_touch_data_android_passive_deepview AS STRING) AS last_attributed_touch_data_android_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_android_url AS STRING) AS last_attributed_touch_data_android_url,
      SAFE_CAST(last_attributed_touch_data_api_open_click AS STRING) AS last_attributed_touch_data_api_open_click,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution AS BOOLEAN) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date AS TIMESTAMP) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword AS STRING) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id,
      SAFE_CAST(last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id AS INT64) AS last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id,
      SAFE_CAST(last_attributed_touch_data_branch_ad_format AS STRING) AS last_attributed_touch_data_branch_ad_format,
      SAFE_CAST(last_attributed_touch_data_campaign AS STRING) AS last_attributed_touch_data_campaign,
      SAFE_CAST(last_attributed_touch_data_campaign_id AS STRING) AS last_attributed_touch_data_campaign_id,
      SAFE_CAST(last_attributed_touch_data_campaign_type AS STRING) AS last_attributed_touch_data_campaign_type,
      SAFE_CAST(last_attributed_touch_data_canonical_url AS STRING) AS last_attributed_touch_data_canonical_url,
      SAFE_CAST(last_attributed_touch_data_channel AS STRING) AS last_attributed_touch_data_channel,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_browser_version AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_browser_version,
      SAFE_CAST(last_attributed_touch_data_click_browser_fingerprint_is_mobile AS STRING) AS last_attributed_touch_data_click_browser_fingerprint_is_mobile,
      SAFE_CAST(last_attributed_touch_data_click_timestamp AS INT64) AS last_attributed_touch_data_click_timestamp,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_collection,
      SAFE_CAST(last_attributed_touch_data_conversion_type AS STRING) AS last_attributed_touch_data_conversion_type,
      SAFE_CAST(last_attributed_touch_data_country_or_region AS STRING) AS last_attributed_touch_data_country_or_region,
      SAFE_CAST(last_attributed_touch_data_creative_id AS STRING) AS last_attributed_touch_data_creative_id,
      SAFE_CAST(last_attributed_touch_data_creative_name AS STRING) AS last_attributed_touch_data_creative_name,
      SAFE_CAST(last_attributed_touch_data_desktop_url AS STRING) AS last_attributed_touch_data_desktop_url,
      SAFE_CAST(last_attributed_touch_data_device_brand_model AS STRING) AS last_attributed_touch_data_device_brand_model,
      SAFE_CAST(last_attributed_touch_data_device_brand_name AS STRING) AS last_attributed_touch_data_device_brand_name,
      SAFE_CAST(last_attributed_touch_data_device_os AS STRING) AS last_attributed_touch_data_device_os,
      SAFE_CAST(last_attributed_touch_data_device_os_version AS STRING) AS last_attributed_touch_data_device_os_version,
      SAFE_CAST(last_attributed_touch_data_fb_data_terms_not_signed AS STRING) AS last_attributed_touch_data_fb_data_terms_not_signed,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_fbclid,
      SAFE_CAST(last_attributed_touch_data_feature AS STRING) AS last_attributed_touch_data_feature,
      SAFE_CAST(last_attributed_touch_data_geo_country_code AS STRING) AS last_attributed_touch_data_geo_country_code,
      SAFE_CAST(last_attributed_touch_data_ios_passive_deepview AS STRING) AS last_attributed_touch_data_ios_passive_deepview,
      SAFE_CAST(last_attributed_touch_data_ios_url AS STRING) AS last_attributed_touch_data_ios_url,
      SAFE_CAST(last_attributed_touch_data_is_mobile_data_terms_signed AS STRING) AS last_attributed_touch_data_is_mobile_data_terms_signed,
      SAFE_CAST(last_attributed_touch_data_keyword AS STRING) AS last_attributed_touch_data_keyword,
      SAFE_CAST(last_attributed_touch_data_keyword_id AS STRING) AS last_attributed_touch_data_keyword_id,
      SAFE_CAST(last_attributed_touch_data_keyword_match_type AS STRING) AS last_attributed_touch_data_keyword_match_type,
      SAFE_CAST(last_attributed_touch_data_link_title AS STRING) AS last_attributed_touch_data_link_title,
      SAFE_CAST(last_attributed_touch_data_link_type AS STRING) AS last_attributed_touch_data_link_type,
      SAFE_CAST(last_attributed_touch_data_one_time_use AS STRING) AS last_attributed_touch_data_one_time_use,
      SAFE_CAST(last_attributed_touch_data_organic_search_url AS STRING) AS last_attributed_touch_data_organic_search_url,
      SAFE_CAST(last_attributed_touch_data_secondary_ad_format AS STRING) AS last_attributed_touch_data_secondary_ad_format,
      SAFE_CAST(last_attributed_touch_data_secondary_publisher AS STRING) AS last_attributed_touch_data_secondary_publisher,
      SAFE_CAST(last_attributed_touch_data_tags AS STRING) AS last_attributed_touch_data_tags,
      SAFE_CAST(last_attributed_touch_data_touch_id AS STRING) AS last_attributed_touch_data_touch_id,
      SAFE_CAST(last_attributed_touch_data_touch_subtype AS STRING) AS last_attributed_touch_data_touch_subtype,
      SAFE_CAST(last_attributed_touch_data_tune_publisher_name AS STRING) AS last_attributed_touch_data_tune_publisher_name,
      SAFE_CAST(last_attributed_touch_data_url AS STRING) AS last_attributed_touch_data_url,
      SAFE_CAST(last_attributed_touch_data_user_data_ip AS STRING) AS last_attributed_touch_data_user_data_ip,
      SAFE_CAST(last_attributed_touch_data_user_data_user_agent AS STRING) AS last_attributed_touch_data_user_data_user_agent,
      SAFE_CAST(last_attributed_touch_data_via_features AS STRING) AS last_attributed_touch_data_via_features,
      SAFE_CAST(last_attributed_touch_timestamp AS INT64) AS last_attributed_touch_timestamp,
      SAFE_CAST(last_attributed_touch_type AS STRING) AS last_attributed_touch_type,
      SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at,
      SAFE_CAST(null AS INT64) AS minutes_from_last_attributed_touch_to_event,
      SAFE_CAST(name AS STRING) AS name,
      SAFE_CAST(origin AS STRING) AS origin,
      SAFE_CAST(original_timestamp AS TIMESTAMP) AS original_timestamp,
      SAFE_CAST(ott AS STRING) AS ott,
      SAFE_CAST(received_at AS TIMESTAMP) AS received_at,
      SAFE_CAST(reengagement_activity_attributed AS BOOLEAN) AS reengagement_activity_attributed,
      SAFE_CAST(reengagement_activity_data_country_code AS STRING) AS reengagement_activity_data_country_code,
      SAFE_CAST(reengagement_activity_event_name AS STRING) AS reengagement_activity_event_name,
      SAFE_CAST(reengagement_activity_timestamp AS INT64) AS reengagement_activity_timestamp,
      SAFE_CAST(reengagement_activity_touch_data_dollar_3p AS STRING) AS reengagement_activity_touch_data_dollar_3p,
      SAFE_CAST(reengagement_activity_touch_data_dollar_fb_data_terms_not_signed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed AS BOOLEAN) AS reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(reengagement_activity_touch_data_tilde_advertising_partner_name AS STRING) AS reengagement_activity_touch_data_tilde_advertising_partner_name,
      SAFE_CAST(seconds_from_install_to_event AS INT64) AS seconds_from_install_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_event,
      SAFE_CAST(null AS INT64) AS seconds_from_last_attributed_touch_to_store_install_begin,
      SAFE_CAST(sent_at AS TIMESTAMP) AS sent_at,
      SAFE_CAST(timestamp AS TIMESTAMP) AS timestamp,
      SAFE_CAST(user_data_aaid AS STRING) AS user_data_aaid,
      SAFE_CAST(user_data_android_id AS STRING) AS user_data_android_id,
      SAFE_CAST(user_data_app_version AS STRING) AS user_data_app_version,
      SAFE_CAST(user_data_brand AS STRING) AS user_data_brand,
      SAFE_CAST(user_data_build AS STRING) AS user_data_build,
      SAFE_CAST(user_data_carrier_name AS STRING) AS user_data_carrier_name,
      SAFE_CAST(user_data_cpu_type AS STRING) AS user_data_cpu_type,
      SAFE_CAST(user_data_developer_identity AS STRING) AS user_data_developer_identity,
      SAFE_CAST(user_data_disable_ad_network_callouts AS BOOLEAN) AS user_data_disable_ad_network_callouts,
      SAFE_CAST(user_data_environment AS STRING) AS user_data_environment,
      SAFE_CAST(user_data_geo_city_code AS INT64) AS user_data_geo_city_code,
      SAFE_CAST(user_data_geo_city_en AS STRING) AS user_data_geo_city_en,
      SAFE_CAST(user_data_geo_continent_code AS STRING) AS user_data_geo_continent_code,
      SAFE_CAST(user_data_geo_country_code AS STRING) AS user_data_geo_country_code,
      SAFE_CAST(user_data_geo_country_en AS STRING) AS user_data_geo_country_en,
      SAFE_CAST(user_data_geo_dma_code AS INT64) AS user_data_geo_dma_code,
      SAFE_CAST(user_data_geo_lat AS FLOAT64) AS user_data_geo_lat,
      SAFE_CAST(user_data_geo_lon AS FLOAT64) AS user_data_geo_lon,
      SAFE_CAST(user_data_geo_region_code AS STRING) AS user_data_geo_region_code,
      SAFE_CAST(user_data_geo_region_en AS STRING) AS user_data_geo_region_en,
      SAFE_CAST(null AS STRING) AS user_data_http_referrer,
      SAFE_CAST(user_data_idfa AS STRING) AS user_data_idfa,
      SAFE_CAST(user_data_idfv AS STRING) AS user_data_idfv,
      SAFE_CAST(user_data_internet_connection_type AS STRING) AS user_data_internet_connection_type,
      SAFE_CAST(user_data_ip AS STRING) AS user_data_ip,
      SAFE_CAST(user_data_language AS STRING) AS user_data_language,
      SAFE_CAST(user_data_limit_ad_tracking AS BOOLEAN) AS user_data_limit_ad_tracking,
      SAFE_CAST(user_data_model AS STRING) AS user_data_model,
      SAFE_CAST(user_data_opted_in AS BOOLEAN) AS user_data_opted_in,
      SAFE_CAST(user_data_opted_in_status AS STRING) AS user_data_opted_in_status,
      SAFE_CAST(user_data_os AS STRING) AS user_data_os,
      SAFE_CAST(user_data_os_version AS STRING) AS user_data_os_version,
      SAFE_CAST(user_data_os_version_android AS STRING) AS user_data_os_version_android,
      SAFE_CAST(user_data_past_cross_platform_ids AS STRING) AS user_data_past_cross_platform_ids,
      SAFE_CAST(user_data_platform AS STRING) AS user_data_platform,
      SAFE_CAST(user_data_private_relay AS BOOLEAN) AS user_data_private_relay,
      SAFE_CAST(user_data_prob_cross_platform_ids AS STRING) AS user_data_prob_cross_platform_ids,
      SAFE_CAST(user_data_screen_height AS INT64) AS user_data_screen_height,
      SAFE_CAST(user_data_screen_width AS INT64) AS user_data_screen_width,
      SAFE_CAST(user_data_sdk_version AS STRING) AS user_data_sdk_version,
      SAFE_CAST(user_data_user_agent AS STRING) AS user_data_user_agent,
      SAFE_CAST(uuid_ts AS TIMESTAMP) AS uuid_ts,
      SAFE_CAST(last_attributed_touch_data_always_deeplink AS STRING) AS last_attributed_touch_data_always_deeplink,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_cross_device,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_fb_data_terms_not_signed,
      SAFE_CAST(null AS BOOLEAN) AS last_attributed_touch_data_dollar_twitter_data_sharing_allowed,
      SAFE_CAST(last_attributed_touch_data_fallback_url AS STRING) AS last_attributed_touch_data_fallback_url,
      SAFE_CAST(last_attributed_touch_data_gclid AS STRING) AS last_attributed_touch_data_gclid,
      SAFE_CAST(null AS STRING) AS last_attributed_touch_data_placement,
      SAFE_CAST(last_attributed_touch_data_view_through AS BOOLEAN) AS last_attributed_touch_data_view_through,
      SAFE_CAST(null AS INT64) AS last_attributed_touch_data_view_time,
      SAFE_CAST(last_attributed_touch_data_view_timestamp AS STRING) AS last_attributed_touch_data_view_timestamp,
      SAFE_CAST(null AS INT64) AS store_install_begin_timestamp,
      SAFE_CAST(null AS INT64) AS referrer_click_timestamp,
      SAFE_CAST(null AS STRING) AS custom_data_opt_in,
      SAFE_CAST(null AS INT64) AS days_from_install_to_opt_in,
      SAFE_CAST(last_attributed_touch_data_domain AS STRING) AS last_attributed_touch_data_domain,
      SAFE_CAST(last_attributed_touch_data_gb AS STRING) AS last_attributed_touch_data_gb,
      SAFE_CAST(last_attributed_touch_data_matching_ttl_s AS STRING) AS last_attributed_touch_data_matching_ttl_s,
      SAFE_CAST(last_attributed_touch_data_referrer AS STRING) AS last_attributed_touch_data_referrer,
      SAFE_CAST(last_attributed_touch_data_platform_source AS STRING) AS last_attributed_touch_data_platform_source,
      SAFE_CAST(last_attributed_touch_data_creation_source AS INT64) AS last_attributed_touch_data_creation_source,
      SAFE_CAST(last_attributed_touch_data_id AS INT64) AS last_attributed_touch_data_id,
      SAFE_CAST(last_attributed_touch_data_marketing AS BOOLEAN) AS last_attributed_touch_data_marketing,
      SAFE_CAST(last_attributed_touch_data_marketing_title AS STRING) AS last_attributed_touch_data_marketing_title,
      SAFE_CAST(last_attributed_touch_data_og_app_id AS STRING) AS last_attributed_touch_data_og_app_id,
      SAFE_CAST(last_attributed_touch_data_og_description AS STRING) AS last_attributed_touch_data_og_description,
      SAFE_CAST(last_attributed_touch_data_og_image_url AS STRING) AS last_attributed_touch_data_og_image_url,
      SAFE_CAST(last_attributed_touch_data_og_title AS STRING) AS last_attributed_touch_data_og_title,
      SAFE_CAST(last_attributed_touch_data_og_type AS STRING) AS last_attributed_touch_data_og_type,
      SAFE_CAST(last_attributed_touch_data_twitter_card AS STRING) AS last_attributed_touch_data_twitter_card,
      SAFE_CAST(last_attributed_touch_data_twitter_description AS STRING) AS last_attributed_touch_data_twitter_description,
      SAFE_CAST(last_attributed_touch_data_twitter_title AS STRING) AS last_attributed_touch_data_twitter_title,
      SAFE_CAST(last_attributed_touch_data_click_id AS STRING) AS last_attributed_touch_data_click_id,
      SAFE_CAST(last_attributed_touch_data_msclkid AS STRING) AS last_attributed_touch_data_msclkid,
      SAFE_CAST(last_attributed_touch_data_web_only AS STRING) AS last_attributed_touch_data_web_only,
      SAFE_CAST(event_data_currency AS STRING) AS event_data_currency,
      SAFE_CAST(event_data_description AS STRING) AS event_data_description,
      SAFE_CAST(event_data_exchange_rate AS INT64) AS event_data_exchange_rate,
      SAFE_CAST(event_data_revenue AS FLOAT64) AS event_data_revenue,
      SAFE_CAST(event_data_revenue_in_local_currency AS INT64) AS event_data_revenue_in_local_currency,
      SAFE_CAST(event_data_revenue_in_usd AS FLOAT64) AS event_data_revenue_in_usd,
      SAFE_CAST(event_data_transaction_id AS STRING) AS event_data_transaction_id,
      SAFE_CAST(user_data_browser AS STRING) AS user_data_browser
      FROM
      branch_io_v2.purchase
      ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }
  dimension: _id {
    sql: ${TABLE}._id ;;
    hidden: yes
  }
  dimension: anonymous_id {
    sql: ${TABLE}.anonymous_id ;;
    hidden: yes
  }
  dimension: attributed {
    sql: ${TABLE}.attributed ;;
    hidden: yes
  }
  dimension: content_items {
    sql: ${TABLE}.content_items ;;
    hidden: yes
  }
  dimension: context_library_name {
    sql: ${TABLE}.context_library_name ;;
    hidden: yes
  }
  dimension: context_library_version {
    sql: ${TABLE}.context_library_version ;;
    hidden: yes
  }
  dimension: cross_device_ott {
    sql: ${TABLE}.cross_device_ott ;;
    hidden: yes
  }
  dimension: custom_data_gateway {
    sql: ${TABLE}.custom_data_gateway ;;
    hidden: yes
  }
  dimension: custom_data_opt_in {
    sql: ${TABLE}.custom_data_opt_in ;;
    hidden: yes
  }
  dimension: custom_data_segment_anonymous_id {
    sql: ${TABLE}.custom_data_segment_anonymous_id ;;
    hidden: yes
  }
  dimension: custom_data_skan_time_window {
    sql: ${TABLE}.custom_data_skan_time_window ;;
    hidden: yes
  }
  dimension: days_from_install_to_opt_in {
    sql: ${TABLE}.days_from_install_to_opt_in ;;
    hidden: yes
  }
  dimension: days_from_last_attributed_touch_to_event {
    sql: ${TABLE}.days_from_last_attributed_touch_to_event ;;
    hidden: yes
  }
  dimension: deep_linked {
    sql: ${TABLE}.deep_linked ;;
    hidden: yes
  }
  dimension: event {
    sql: ${TABLE}.event ;;
    hidden: yes
  }
  dimension: event_days_from_timestamp {
    sql: ${TABLE}.event_days_from_timestamp ;;
    hidden: yes
  }
  dimension: event_text {
    sql: ${TABLE}.event_text ;;
    hidden: yes
  }
  dimension: event_timestamp {
    sql: ${TABLE}.event_timestamp ;;
    hidden: yes
  }
  dimension: existing_user {
    sql: ${TABLE}.existing_user ;;
    hidden: yes
  }
  dimension: first_event_for_user {
    sql: ${TABLE}.first_event_for_user ;;
    hidden: yes
  }
  dimension: hours_from_last_attributed_touch_to_event {
    sql: ${TABLE}.hours_from_last_attributed_touch_to_event ;;
    hidden: yes
  }
  dimension: id {
    sql: ${TABLE}.id ;;
    hidden: yes
  }
  dimension: install_activity_attributed {
    sql: ${TABLE}.install_activity_attributed ;;
    hidden: yes
  }
  dimension: install_activity_data_country_code {
    sql: ${TABLE}.install_activity_data_country_code ;;
    hidden: yes
  }
  dimension: install_activity_event_name {
    sql: ${TABLE}.install_activity_event_name ;;
    hidden: yes
  }
  dimension: install_activity_timestamp {
    sql: ${TABLE}.install_activity_timestamp ;;
    hidden: yes
  }
  dimension: install_activity_touch_data_dollar_3p {
    sql: ${TABLE}.install_activity_touch_data_dollar_3p ;;
    hidden: yes
  }
  dimension: install_activity_touch_data_dollar_fb_data_terms_not_signed {
    sql: ${TABLE}.install_activity_touch_data_dollar_fb_data_terms_not_signed ;;
    hidden: yes
  }
  dimension: install_activity_touch_data_dollar_twitter_data_sharing_allowed {
    sql: ${TABLE}.install_activity_touch_data_dollar_twitter_data_sharing_allowed ;;
    hidden: yes
  }
  dimension: install_activity_touch_data_tilde_advertising_partner_name {
    sql: ${TABLE}.install_activity_touch_data_tilde_advertising_partner_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_3p {
    sql: ${TABLE}.last_attributed_touch_data_3p ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ad_id {
    sql: ${TABLE}.last_attributed_touch_data_ad_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ad_name {
    sql: ${TABLE}.last_attributed_touch_data_ad_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ad_objective_name {
    sql: ${TABLE}.last_attributed_touch_data_ad_objective_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ad_set_id {
    sql: ${TABLE}.last_attributed_touch_data_ad_set_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ad_set_name {
    sql: ${TABLE}.last_attributed_touch_data_ad_set_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_advertising_account_id {
    sql: ${TABLE}.last_attributed_touch_data_advertising_account_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_advertising_partner_id {
    sql: ${TABLE}.last_attributed_touch_data_advertising_partner_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_advertising_partner_name {
    sql: ${TABLE}.last_attributed_touch_data_advertising_partner_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_always_deeplink {
    sql: ${TABLE}.last_attributed_touch_data_always_deeplink ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_android_passive_deepview {
    sql: ${TABLE}.last_attributed_touch_data_android_passive_deepview ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_android_url {
    sql: ${TABLE}.last_attributed_touch_data_android_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_api_open_click {
    sql: ${TABLE}.last_attributed_touch_data_api_open_click ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id {
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_branch_ad_format {
    sql: ${TABLE}.last_attributed_touch_data_branch_ad_format ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_campaign {
    sql: ${TABLE}.last_attributed_touch_data_campaign ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_campaign_id {
    sql: ${TABLE}.last_attributed_touch_data_campaign_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_campaign_type {
    sql: ${TABLE}.last_attributed_touch_data_campaign_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_canonical_url {
    sql: ${TABLE}.last_attributed_touch_data_canonical_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_channel {
    sql: ${TABLE}.last_attributed_touch_data_channel ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_click_browser_fingerprint_browser {
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_browser ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_click_browser_fingerprint_browser_version {
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_browser_version ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_click_browser_fingerprint_is_mobile {
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_is_mobile ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_click_id {
    sql: ${TABLE}.last_attributed_touch_data_click_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_click_timestamp {
    sql: ${TABLE}.last_attributed_touch_data_click_timestamp ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_collection {
    sql: ${TABLE}.last_attributed_touch_data_collection ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_conversion_type {
    sql: ${TABLE}.last_attributed_touch_data_conversion_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_country_or_region {
    sql: ${TABLE}.last_attributed_touch_data_country_or_region ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_creation_source {
    sql: ${TABLE}.last_attributed_touch_data_creation_source ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_creative_id {
    sql: ${TABLE}.last_attributed_touch_data_creative_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_creative_name {
    sql: ${TABLE}.last_attributed_touch_data_creative_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_desktop_url {
    sql: ${TABLE}.last_attributed_touch_data_desktop_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_device_brand_model {
    sql: ${TABLE}.last_attributed_touch_data_device_brand_model ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_device_brand_name {
    sql: ${TABLE}.last_attributed_touch_data_device_brand_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_device_os {
    sql: ${TABLE}.last_attributed_touch_data_device_os ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_device_os_version {
    sql: ${TABLE}.last_attributed_touch_data_device_os_version ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_dollar_cross_device {
    sql: ${TABLE}.last_attributed_touch_data_dollar_cross_device ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_dollar_fb_data_terms_not_signed {
    sql: ${TABLE}.last_attributed_touch_data_dollar_fb_data_terms_not_signed ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_dollar_twitter_data_sharing_allowed {
    sql: ${TABLE}.last_attributed_touch_data_dollar_twitter_data_sharing_allowed ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_domain {
    sql: ${TABLE}.last_attributed_touch_data_domain ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_fallback_url {
    sql: ${TABLE}.last_attributed_touch_data_fallback_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_fb_data_terms_not_signed {
    sql: ${TABLE}.last_attributed_touch_data_fb_data_terms_not_signed ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_fbclid {
    sql: ${TABLE}.last_attributed_touch_data_fbclid ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_feature {
    sql: ${TABLE}.last_attributed_touch_data_feature ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_gb {
    sql: ${TABLE}.last_attributed_touch_data_gb ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_gclid {
    sql: ${TABLE}.last_attributed_touch_data_gclid ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_geo_country_code {
    sql: ${TABLE}.last_attributed_touch_data_geo_country_code ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_id {
    sql: ${TABLE}.last_attributed_touch_data_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ios_passive_deepview {
    sql: ${TABLE}.last_attributed_touch_data_ios_passive_deepview ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_ios_url {
    sql: ${TABLE}.last_attributed_touch_data_ios_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_is_mobile_data_terms_signed {
    sql: ${TABLE}.last_attributed_touch_data_is_mobile_data_terms_signed ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_keyword {
    sql: ${TABLE}.last_attributed_touch_data_keyword ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_keyword_id {
    sql: ${TABLE}.last_attributed_touch_data_keyword_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_keyword_match_type {
    sql: ${TABLE}.last_attributed_touch_data_keyword_match_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_link_title {
    sql: ${TABLE}.last_attributed_touch_data_link_title ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_link_type {
    sql: ${TABLE}.last_attributed_touch_data_link_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_marketing {
    sql: ${TABLE}.last_attributed_touch_data_marketing ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_marketing_title {
    sql: ${TABLE}.last_attributed_touch_data_marketing_title ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_matching_ttl_s {
    sql: ${TABLE}.last_attributed_touch_data_matching_ttl_s ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_msclkid {
    sql: ${TABLE}.last_attributed_touch_data_msclkid ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_og_app_id {
    sql: ${TABLE}.last_attributed_touch_data_og_app_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_og_description {
    sql: ${TABLE}.last_attributed_touch_data_og_description ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_og_image_url {
    sql: ${TABLE}.last_attributed_touch_data_og_image_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_og_title {
    sql: ${TABLE}.last_attributed_touch_data_og_title ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_og_type {
    sql: ${TABLE}.last_attributed_touch_data_og_type ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_one_time_use {
    sql: ${TABLE}.last_attributed_touch_data_one_time_use ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_organic_search_url {
    sql: ${TABLE}.last_attributed_touch_data_organic_search_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_placement {
    sql: ${TABLE}.last_attributed_touch_data_placement ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_platform_source {
    sql: ${TABLE}.last_attributed_touch_data_platform_source ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_referrer {
    sql: ${TABLE}.last_attributed_touch_data_referrer ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_secondary_ad_format {
    sql: ${TABLE}.last_attributed_touch_data_secondary_ad_format ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_secondary_publisher {
    sql: ${TABLE}.last_attributed_touch_data_secondary_publisher ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_tags {
    sql: ${TABLE}.last_attributed_touch_data_tags ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_touch_id {
    sql: ${TABLE}.last_attributed_touch_data_touch_id ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_touch_subtype {
    sql: ${TABLE}.last_attributed_touch_data_touch_subtype ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_tune_publisher_name {
    sql: ${TABLE}.last_attributed_touch_data_tune_publisher_name ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_twitter_card {
    sql: ${TABLE}.last_attributed_touch_data_twitter_card ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_twitter_description {
    sql: ${TABLE}.last_attributed_touch_data_twitter_description ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_twitter_title {
    sql: ${TABLE}.last_attributed_touch_data_twitter_title ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_url {
    sql: ${TABLE}.last_attributed_touch_data_url ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_user_data_ip {
    sql: ${TABLE}.last_attributed_touch_data_user_data_ip ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_user_data_user_agent {
    sql: ${TABLE}.last_attributed_touch_data_user_data_user_agent ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_via_features {
    sql: ${TABLE}.last_attributed_touch_data_via_features ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_view_through {
    sql: ${TABLE}.last_attributed_touch_data_view_through ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_view_time {
    sql: ${TABLE}.last_attributed_touch_data_view_time ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_view_timestamp {
    sql: ${TABLE}.last_attributed_touch_data_view_timestamp ;;
    hidden: yes
  }
  dimension: last_attributed_touch_data_web_only {
    sql: ${TABLE}.last_attributed_touch_data_web_only ;;
    hidden: yes
  }
  dimension: last_attributed_touch_timestamp {
    sql: ${TABLE}.last_attributed_touch_timestamp ;;
    hidden: yes
  }
  dimension: last_attributed_touch_type {
    sql: ${TABLE}.last_attributed_touch_type ;;
    hidden: yes
  }
  dimension: loaded_at {
    sql: ${TABLE}.loaded_at ;;
    hidden: yes
  }
  dimension: minutes_from_last_attributed_touch_to_event {
    sql: ${TABLE}.minutes_from_last_attributed_touch_to_event ;;
    hidden: yes
  }
  dimension: name {
    sql: ${TABLE}.name ;;
    hidden: yes
  }
  dimension: origin {
    sql: ${TABLE}.origin ;;
    hidden: yes
  }
  dimension: original_timestamp {
    sql: ${TABLE}.original_timestamp ;;
    hidden: yes
  }
  dimension: ott {
    sql: ${TABLE}.ott ;;
    hidden: yes
  }
  dimension: received_at {
    sql: ${TABLE}.received_at ;;
    hidden: yes
  }
  dimension: reengagement_activity_attributed {
    sql: ${TABLE}.reengagement_activity_attributed ;;
    hidden: yes
  }
  dimension: reengagement_activity_data_country_code {
    sql: ${TABLE}.reengagement_activity_data_country_code ;;
    hidden: yes
  }
  dimension: reengagement_activity_event_name {
    sql: ${TABLE}.reengagement_activity_event_name ;;
    hidden: yes
  }
  dimension: reengagement_activity_timestamp {
    sql: ${TABLE}.reengagement_activity_timestamp ;;
    hidden: yes
  }
  dimension: reengagement_activity_touch_data_dollar_3p {
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_3p ;;
    hidden: yes
  }
  dimension: reengagement_activity_touch_data_dollar_fb_data_terms_not_signed {
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_fb_data_terms_not_signed ;;
    hidden: yes
  }
  dimension: reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed {
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed ;;
    hidden: yes
  }
  dimension: reengagement_activity_touch_data_tilde_advertising_partner_name {
    sql: ${TABLE}.reengagement_activity_touch_data_tilde_advertising_partner_name ;;
    hidden: yes
  }
  dimension: referrer_click_timestamp {
    sql: ${TABLE}.referrer_click_timestamp ;;
    hidden: yes
  }
  dimension: seconds_from_install_to_event {
    sql: ${TABLE}.seconds_from_install_to_event ;;
    hidden: yes
  }
  dimension: seconds_from_last_attributed_touch_to_event {
    sql: ${TABLE}.seconds_from_last_attributed_touch_to_event ;;
    hidden: yes
  }
  dimension: seconds_from_last_attributed_touch_to_store_install_begin {
    sql: ${TABLE}.seconds_from_last_attributed_touch_to_store_install_begin ;;
    hidden: yes
  }
  dimension: sent_at {
    sql: ${TABLE}.sent_at ;;
    hidden: yes
  }
  dimension: store_install_begin_timestamp {
    sql: ${TABLE}.store_install_begin_timestamp ;;
    hidden: yes
  }
  dimension: timestamp {
    sql: ${TABLE}.timestamp ;;
    hidden: yes
  }
  dimension: user_data_aaid {
    sql: ${TABLE}.user_data_aaid ;;
    hidden: yes
  }
  dimension: user_data_android_id {
    sql: ${TABLE}.user_data_android_id ;;
    hidden: yes
  }
  dimension: user_data_app_version {
    sql: ${TABLE}.user_data_app_version ;;
    hidden: yes
  }
  dimension: user_data_brand {
    sql: ${TABLE}.user_data_brand ;;
    hidden: yes
  }
  dimension: user_data_build {
    sql: ${TABLE}.user_data_build ;;
    hidden: yes
  }
  dimension: user_data_carrier_name {
    sql: ${TABLE}.user_data_carrier_name ;;
    hidden: yes
  }
  dimension: user_data_cpu_type {
    sql: ${TABLE}.user_data_cpu_type ;;
    hidden: yes
  }
  dimension: user_data_developer_identity {
    sql: ${TABLE}.user_data_developer_identity ;;
    hidden: yes
  }
  dimension: user_data_disable_ad_network_callouts {
    sql: ${TABLE}.user_data_disable_ad_network_callouts ;;
    hidden: yes
  }
  dimension: user_data_environment {
    sql: ${TABLE}.user_data_environment ;;
    hidden: yes
  }
  dimension: user_data_geo_city_code {
    sql: ${TABLE}.user_data_geo_city_code ;;
    hidden: yes
  }
  dimension: user_data_geo_city_en {
    sql: ${TABLE}.user_data_geo_city_en ;;
    hidden: yes
  }
  dimension: user_data_geo_continent_code {
    sql: ${TABLE}.user_data_geo_continent_code ;;
    hidden: yes
  }
  dimension: user_data_geo_country_code {
    sql: ${TABLE}.user_data_geo_country_code ;;
    hidden: yes
  }
  dimension: user_data_geo_country_en {
    sql: ${TABLE}.user_data_geo_country_en ;;
    hidden: yes
  }
  dimension: user_data_geo_dma_code {
    sql: ${TABLE}.user_data_geo_dma_code ;;
    hidden: yes
  }
  dimension: user_data_geo_lat {
    sql: ${TABLE}.user_data_geo_lat ;;
    hidden: yes
  }
  dimension: user_data_geo_lon {
    sql: ${TABLE}.user_data_geo_lon ;;
    hidden: yes
  }
  dimension: user_data_geo_region_code {
    sql: ${TABLE}.user_data_geo_region_code ;;
    hidden: yes
  }
  dimension: user_data_geo_region_en {
    sql: ${TABLE}.user_data_geo_region_en ;;
    hidden: yes
  }
  dimension: user_data_http_referrer {
    sql: ${TABLE}.user_data_http_referrer ;;
    hidden: yes
  }
  dimension: user_data_idfa {
    sql: ${TABLE}.user_data_idfa ;;
    hidden: yes
  }
  dimension: user_data_idfv {
    sql: ${TABLE}.user_data_idfv ;;
    hidden: yes
  }
  dimension: user_data_internet_connection_type {
    sql: ${TABLE}.user_data_internet_connection_type ;;
    hidden: yes
  }
  dimension: user_data_ip {
    sql: ${TABLE}.user_data_ip ;;
    hidden: yes
  }
  dimension: user_data_language {
    sql: ${TABLE}.user_data_language ;;
    hidden: yes
  }
  dimension: user_data_limit_ad_tracking {
    sql: ${TABLE}.user_data_limit_ad_tracking ;;
    hidden: yes
  }
  dimension: user_data_model {
    sql: ${TABLE}.user_data_model ;;
    hidden: yes
  }
  dimension: user_data_opted_in {
    sql: ${TABLE}.user_data_opted_in ;;
    hidden: yes
  }
  dimension: user_data_opted_in_status {
    sql: ${TABLE}.user_data_opted_in_status ;;
    hidden: yes
  }
  dimension: user_data_os {
    sql: ${TABLE}.user_data_os ;;
    hidden: yes
  }
  dimension: user_data_os_version {
    sql: ${TABLE}.user_data_os_version ;;
    hidden: yes
  }
  dimension: user_data_os_version_android {
    sql: ${TABLE}.user_data_os_version_android ;;
    hidden: yes
  }
  dimension: user_data_past_cross_platform_ids {
    sql: ${TABLE}.user_data_past_cross_platform_ids ;;
    hidden: yes
  }
  dimension: user_data_platform {
    sql: ${TABLE}.user_data_platform ;;
    hidden: yes
  }
  dimension: user_data_private_relay {
    sql: ${TABLE}.user_data_private_relay ;;
    hidden: yes
  }
  dimension: user_data_prob_cross_platform_ids {
    sql: ${TABLE}.user_data_prob_cross_platform_ids ;;
    hidden: yes
  }
  dimension: user_data_screen_height {
    sql: ${TABLE}.user_data_screen_height ;;
    hidden: yes
  }
  dimension: user_data_screen_width {
    sql: ${TABLE}.user_data_screen_width ;;
    hidden: yes
  }
  dimension: user_data_sdk_version {
    sql: ${TABLE}.user_data_sdk_version ;;
    hidden: yes
  }
  dimension: user_data_user_agent {
    sql: ${TABLE}.user_data_user_agent ;;
    hidden: yes
  }
  dimension: uuid_ts {
    sql: ${TABLE}.uuid_ts ;;
    hidden: yes
  }
  dimension: event_data_currency {
    sql: ${TABLE}.event_data_currency ;;
    hidden: yes
  }
  dimension: event_data_description {
    sql: ${TABLE}.event_data_description ;;
    hidden: yes
  }
  dimension: event_data_exchange_rate {
    sql: ${TABLE}.event_data_exchange_rate ;;
    hidden: yes
  }
  dimension: event_data_revenue {
    sql: ${TABLE}.event_data_revenue ;;
    hidden: yes
  }
  dimension: event_data_revenue_in_local_currency {
    sql: ${TABLE}.event_data_revenue_in_local_currency ;;
    hidden: yes
  }
  dimension: event_data_revenue_in_usd {
    sql: ${TABLE}.event_data_revenue_in_usd ;;
    hidden: yes
  }
  dimension: event_data_transaction_id {
    sql: ${TABLE}.event_data_transaction_id ;;
    hidden: yes
  }
  dimension: user_data_browser {
    sql: ${TABLE}.user_data_browser ;;
    hidden: yes
  }
}
