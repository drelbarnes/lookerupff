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
    type: string
    sql: ${TABLE}._id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: attributed {
    type: string
    sql: ${TABLE}.attributed ;;
  }

  dimension: content_items {
    type: string
    sql: ${TABLE}.content_items ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: cross_device_ott {
    type: string
    sql: ${TABLE}.cross_device_ott ;;
  }

  dimension: custom_data_gateway {
    type: string
    sql: ${TABLE}.custom_data_gateway ;;
  }

  dimension: custom_data_opt_in {
    type: string
    sql: ${TABLE}.custom_data_opt_in ;;
  }

  dimension: custom_data_segment_anonymous_id {
    type: string
    sql: ${TABLE}.custom_data_segment_anonymous_id ;;
  }

  dimension: custom_data_skan_time_window {
    type: string
    sql: ${TABLE}.custom_data_skan_time_window ;;
  }

  dimension: days_from_install_to_opt_in {
    type: number
    sql: ${TABLE}.days_from_install_to_opt_in ;;
  }

  dimension: days_from_last_attributed_touch_to_event {
    type: number
    sql: ${TABLE}.days_from_last_attributed_touch_to_event ;;
  }

  dimension: deep_linked {
    type: string
    sql: ${TABLE}.deep_linked ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_days_from_timestamp {
    type: number
    sql: ${TABLE}.event_days_from_timestamp ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: event_timestamp {
    type: number
    sql: ${TABLE}.event_timestamp ;;
  }

  dimension: existing_user {
    type: string
    sql: ${TABLE}.existing_user ;;
  }

  dimension: first_event_for_user {
    type: string
    sql: ${TABLE}.first_event_for_user ;;
  }

  dimension: hours_from_last_attributed_touch_to_event {
    type: number
    sql: ${TABLE}.hours_from_last_attributed_touch_to_event ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: install_activity_attributed {
    type: yesno
    sql: ${TABLE}.install_activity_attributed ;;
  }

  dimension: install_activity_data_country_code {
    type: string
    sql: ${TABLE}.install_activity_data_country_code ;;
  }

  dimension: install_activity_event_name {
    type: string
    sql: ${TABLE}.install_activity_event_name ;;
  }

  dimension: install_activity_timestamp {
    type: number
    sql: ${TABLE}.install_activity_timestamp ;;
  }

  dimension: install_activity_touch_data_dollar_3p {
    type: string
    sql: ${TABLE}.install_activity_touch_data_dollar_3p ;;
  }

  dimension: install_activity_touch_data_dollar_fb_data_terms_not_signed {
    type: yesno
    sql: ${TABLE}.install_activity_touch_data_dollar_fb_data_terms_not_signed ;;
  }

  dimension: install_activity_touch_data_dollar_twitter_data_sharing_allowed {
    type: yesno
    sql: ${TABLE}.install_activity_touch_data_dollar_twitter_data_sharing_allowed ;;
  }

  dimension: install_activity_touch_data_tilde_advertising_partner_name {
    type: string
    sql: ${TABLE}.install_activity_touch_data_tilde_advertising_partner_name ;;
  }

  dimension: last_attributed_touch_data_3p {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_3p ;;
  }

  dimension: last_attributed_touch_data_ad_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ad_id ;;
  }

  dimension: last_attributed_touch_data_ad_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ad_name ;;
  }

  dimension: last_attributed_touch_data_ad_objective_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ad_objective_name ;;
  }

  dimension: last_attributed_touch_data_ad_set_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ad_set_id ;;
  }

  dimension: last_attributed_touch_data_ad_set_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ad_set_name ;;
  }

  dimension: last_attributed_touch_data_advertising_account_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_advertising_account_id ;;
  }

  dimension: last_attributed_touch_data_advertising_partner_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_advertising_partner_id ;;
  }

  dimension: last_attributed_touch_data_advertising_partner_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_advertising_partner_name ;;
  }

  dimension: last_attributed_touch_data_always_deeplink {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_always_deeplink ;;
  }

  dimension: last_attributed_touch_data_android_passive_deepview {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_android_passive_deepview ;;
  }

  dimension: last_attributed_touch_data_android_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_android_url ;;
  }

  dimension: last_attributed_touch_data_api_open_click {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_api_open_click ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_id ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_adgroup_name ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_attribution ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_id ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_campaign_name ;;
  }

  dimension_group: last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_click_date ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_conversion_type ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_country_or_region ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_keyword_id ;;
  }

  dimension: last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_apple_search_ads_attribution_response_iad_org_id ;;
  }

  dimension: last_attributed_touch_data_branch_ad_format {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_branch_ad_format ;;
  }

  dimension: last_attributed_touch_data_campaign {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_campaign ;;
  }

  dimension: last_attributed_touch_data_campaign_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_campaign_id ;;
  }

  dimension: last_attributed_touch_data_campaign_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_campaign_type ;;
  }

  dimension: last_attributed_touch_data_canonical_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_canonical_url ;;
  }

  dimension: last_attributed_touch_data_channel {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_channel ;;
  }

  dimension: last_attributed_touch_data_click_browser_fingerprint_browser {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_browser ;;
  }

  dimension: last_attributed_touch_data_click_browser_fingerprint_browser_version {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_browser_version ;;
  }

  dimension: last_attributed_touch_data_click_browser_fingerprint_is_mobile {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_click_browser_fingerprint_is_mobile ;;
  }

  dimension: last_attributed_touch_data_click_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_click_id ;;
  }

  dimension: last_attributed_touch_data_click_timestamp {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_click_timestamp ;;
  }

  dimension: last_attributed_touch_data_collection {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_collection ;;
  }

  dimension: last_attributed_touch_data_conversion_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_conversion_type ;;
  }

  dimension: last_attributed_touch_data_country_or_region {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_country_or_region ;;
  }

  dimension: last_attributed_touch_data_creation_source {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_creation_source ;;
  }

  dimension: last_attributed_touch_data_creative_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_creative_id ;;
  }

  dimension: last_attributed_touch_data_creative_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_creative_name ;;
  }

  dimension: last_attributed_touch_data_desktop_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_desktop_url ;;
  }

  dimension: last_attributed_touch_data_device_brand_model {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_device_brand_model ;;
  }

  dimension: last_attributed_touch_data_device_brand_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_device_brand_name ;;
  }

  dimension: last_attributed_touch_data_device_os {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_device_os ;;
  }

  dimension: last_attributed_touch_data_device_os_version {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_device_os_version ;;
  }

  dimension: last_attributed_touch_data_dollar_cross_device {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_dollar_cross_device ;;
  }

  dimension: last_attributed_touch_data_dollar_fb_data_terms_not_signed {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_dollar_fb_data_terms_not_signed ;;
  }

  dimension: last_attributed_touch_data_dollar_twitter_data_sharing_allowed {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_dollar_twitter_data_sharing_allowed ;;
  }

  dimension: last_attributed_touch_data_domain {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_domain ;;
  }

  dimension: last_attributed_touch_data_fallback_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_fallback_url ;;
  }

  dimension: last_attributed_touch_data_fb_data_terms_not_signed {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_fb_data_terms_not_signed ;;
  }

  dimension: last_attributed_touch_data_fbclid {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_fbclid ;;
  }

  dimension: last_attributed_touch_data_feature {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_feature ;;
  }

  dimension: last_attributed_touch_data_gb {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_gb ;;
  }

  dimension: last_attributed_touch_data_gclid {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_gclid ;;
  }

  dimension: last_attributed_touch_data_geo_country_code {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_geo_country_code ;;
  }

  dimension: last_attributed_touch_data_id {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_id ;;
  }

  dimension: last_attributed_touch_data_ios_passive_deepview {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ios_passive_deepview ;;
  }

  dimension: last_attributed_touch_data_ios_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_ios_url ;;
  }

  dimension: last_attributed_touch_data_is_mobile_data_terms_signed {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_is_mobile_data_terms_signed ;;
  }

  dimension: last_attributed_touch_data_keyword {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_keyword ;;
  }

  dimension: last_attributed_touch_data_keyword_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_keyword_id ;;
  }

  dimension: last_attributed_touch_data_keyword_match_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_keyword_match_type ;;
  }

  dimension: last_attributed_touch_data_link_title {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_link_title ;;
  }

  dimension: last_attributed_touch_data_link_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_link_type ;;
  }

  dimension: last_attributed_touch_data_marketing {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_marketing ;;
  }

  dimension: last_attributed_touch_data_marketing_title {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_marketing_title ;;
  }

  dimension: last_attributed_touch_data_matching_ttl_s {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_matching_ttl_s ;;
  }

  dimension: last_attributed_touch_data_msclkid {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_msclkid ;;
  }

  dimension: last_attributed_touch_data_og_app_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_og_app_id ;;
  }

  dimension: last_attributed_touch_data_og_description {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_og_description ;;
  }

  dimension: last_attributed_touch_data_og_image_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_og_image_url ;;
  }

  dimension: last_attributed_touch_data_og_title {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_og_title ;;
  }

  dimension: last_attributed_touch_data_og_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_og_type ;;
  }

  dimension: last_attributed_touch_data_one_time_use {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_one_time_use ;;
  }

  dimension: last_attributed_touch_data_organic_search_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_organic_search_url ;;
  }

  dimension: last_attributed_touch_data_placement {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_placement ;;
  }

  dimension: last_attributed_touch_data_platform_source {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_platform_source ;;
  }

  dimension: last_attributed_touch_data_referrer {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_referrer ;;
  }

  dimension: last_attributed_touch_data_secondary_ad_format {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_secondary_ad_format ;;
  }

  dimension: last_attributed_touch_data_secondary_publisher {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_secondary_publisher ;;
  }

  dimension: last_attributed_touch_data_tags {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tags ;;
  }

  dimension: last_attributed_touch_data_touch_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_touch_id ;;
  }

  dimension: last_attributed_touch_data_touch_subtype {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_touch_subtype ;;
  }

  dimension: last_attributed_touch_data_tune_publisher_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tune_publisher_name ;;
  }

  dimension: last_attributed_touch_data_twitter_card {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_twitter_card ;;
  }

  dimension: last_attributed_touch_data_twitter_description {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_twitter_description ;;
  }

  dimension: last_attributed_touch_data_twitter_title {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_twitter_title ;;
  }

  dimension: last_attributed_touch_data_url {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_url ;;
  }

  dimension: last_attributed_touch_data_user_data_ip {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_user_data_ip ;;
  }

  dimension: last_attributed_touch_data_user_data_user_agent {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_user_data_user_agent ;;
  }

  dimension: last_attributed_touch_data_via_features {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_via_features ;;
  }

  dimension: last_attributed_touch_data_view_through {
    type: yesno
    sql: ${TABLE}.last_attributed_touch_data_view_through ;;
  }

  dimension: last_attributed_touch_data_view_time {
    type: number
    sql: ${TABLE}.last_attributed_touch_data_view_time ;;
  }

  dimension: last_attributed_touch_data_view_timestamp {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_view_timestamp ;;
  }

  dimension: last_attributed_touch_data_web_only {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_web_only ;;
  }

  dimension: last_attributed_touch_timestamp {
    type: number
    sql: ${TABLE}.last_attributed_touch_timestamp ;;
  }

  dimension: last_attributed_touch_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_type ;;
  }

  dimension_group: loaded_at {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: minutes_from_last_attributed_touch_to_event {
    type: number
    sql: ${TABLE}.minutes_from_last_attributed_touch_to_event ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: origin {
    type: string
    sql: ${TABLE}.origin ;;
  }

  dimension_group: original_timestamp {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: ott {
    type: string
    sql: ${TABLE}.ott ;;
  }

  dimension_group: received_at {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.received_at ;;
  }

  dimension: reengagement_activity_attributed {
    type: yesno
    sql: ${TABLE}.reengagement_activity_attributed ;;
  }

  dimension: reengagement_activity_data_country_code {
    type: string
    sql: ${TABLE}.reengagement_activity_data_country_code ;;
  }

  dimension: reengagement_activity_event_name {
    type: string
    sql: ${TABLE}.reengagement_activity_event_name ;;
  }

  dimension: reengagement_activity_timestamp {
    type: number
    sql: ${TABLE}.reengagement_activity_timestamp ;;
  }

  dimension: reengagement_activity_touch_data_dollar_3p {
    type: string
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_3p ;;
  }

  dimension: reengagement_activity_touch_data_dollar_fb_data_terms_not_signed {
    type: yesno
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_fb_data_terms_not_signed ;;
  }

  dimension: reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed {
    type: yesno
    sql: ${TABLE}.reengagement_activity_touch_data_dollar_twitter_data_sharing_allowed ;;
  }

  dimension: reengagement_activity_touch_data_tilde_advertising_partner_name {
    type: string
    sql: ${TABLE}.reengagement_activity_touch_data_tilde_advertising_partner_name ;;
  }

  dimension: referrer_click_timestamp {
    type: number
    sql: ${TABLE}.referrer_click_timestamp ;;
  }

  dimension: seconds_from_install_to_event {
    type: number
    sql: ${TABLE}.seconds_from_install_to_event ;;
  }

  dimension: seconds_from_last_attributed_touch_to_event {
    type: number
    sql: ${TABLE}.seconds_from_last_attributed_touch_to_event ;;
  }

  dimension: seconds_from_last_attributed_touch_to_store_install_begin {
    type: number
    sql: ${TABLE}.seconds_from_last_attributed_touch_to_store_install_begin ;;
  }

  dimension_group: sent_at {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.sent_at ;;
  }

  dimension: store_install_begin_timestamp {
    type: number
    sql: ${TABLE}.store_install_begin_timestamp ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_data_aaid {
    type: string
    sql: ${TABLE}.user_data_aaid ;;
  }

  dimension: user_data_android_id {
    type: string
    sql: ${TABLE}.user_data_android_id ;;
  }

  dimension: user_data_app_version {
    type: string
    sql: ${TABLE}.user_data_app_version ;;
  }

  dimension: user_data_brand {
    type: string
    sql: ${TABLE}.user_data_brand ;;
  }

  dimension: user_data_build {
    type: string
    sql: ${TABLE}.user_data_build ;;
  }

  dimension: user_data_carrier_name {
    type: string
    sql: ${TABLE}.user_data_carrier_name ;;
  }

  dimension: user_data_cpu_type {
    type: string
    sql: ${TABLE}.user_data_cpu_type ;;
  }

  dimension: user_data_developer_identity {
    type: string
    sql: ${TABLE}.user_data_developer_identity ;;
  }

  dimension: user_data_disable_ad_network_callouts {
    type: yesno
    sql: ${TABLE}.user_data_disable_ad_network_callouts ;;
  }

  dimension: user_data_environment {
    type: string
    sql: ${TABLE}.user_data_environment ;;
  }

  dimension: user_data_geo_city_code {
    type: number
    sql: ${TABLE}.user_data_geo_city_code ;;
  }

  dimension: user_data_geo_city_en {
    type: string
    sql: ${TABLE}.user_data_geo_city_en ;;
  }

  dimension: user_data_geo_continent_code {
    type: string
    sql: ${TABLE}.user_data_geo_continent_code ;;
  }

  dimension: user_data_geo_country_code {
    type: string
    sql: ${TABLE}.user_data_geo_country_code ;;
  }

  dimension: user_data_geo_country_en {
    type: string
    sql: ${TABLE}.user_data_geo_country_en ;;
  }

  dimension: user_data_geo_dma_code {
    type: number
    sql: ${TABLE}.user_data_geo_dma_code ;;
  }

  dimension: user_data_geo_lat {
    type: number
    sql: ${TABLE}.user_data_geo_lat ;;
  }

  dimension: user_data_geo_lon {
    type: number
    sql: ${TABLE}.user_data_geo_lon ;;
  }

  dimension: user_data_geo_region_code {
    type: string
    sql: ${TABLE}.user_data_geo_region_code ;;
  }

  dimension: user_data_geo_region_en {
    type: string
    sql: ${TABLE}.user_data_geo_region_en ;;
  }

  dimension: user_data_http_referrer {
    type: string
    sql: ${TABLE}.user_data_http_referrer ;;
  }

  dimension: user_data_idfa {
    type: string
    sql: ${TABLE}.user_data_idfa ;;
  }

  dimension: user_data_idfv {
    type: string
    sql: ${TABLE}.user_data_idfv ;;
  }

  dimension: user_data_internet_connection_type {
    type: string
    sql: ${TABLE}.user_data_internet_connection_type ;;
  }

  dimension: user_data_ip {
    type: string
    sql: ${TABLE}.user_data_ip ;;
  }

  dimension: user_data_language {
    type: string
    sql: ${TABLE}.user_data_language ;;
  }

  dimension: user_data_limit_ad_tracking {
    type: yesno
    sql: ${TABLE}.user_data_limit_ad_tracking ;;
  }

  dimension: user_data_model {
    type: string
    sql: ${TABLE}.user_data_model ;;
  }

  dimension: user_data_opted_in {
    type: yesno
    sql: ${TABLE}.user_data_opted_in ;;
  }

  dimension: user_data_opted_in_status {
    type: string
    sql: ${TABLE}.user_data_opted_in_status ;;
  }

  dimension: user_data_os {
    type: string
    sql: ${TABLE}.user_data_os ;;
  }

  dimension: user_data_os_version {
    type: string
    sql: ${TABLE}.user_data_os_version ;;
  }

  dimension: user_data_os_version_android {
    type: string
    sql: ${TABLE}.user_data_os_version_android ;;
  }

  dimension: user_data_past_cross_platform_ids {
    type: string
    sql: ${TABLE}.user_data_past_cross_platform_ids ;;
  }

  dimension: user_data_platform {
    type: string
    sql: ${TABLE}.user_data_platform ;;
  }

  dimension: user_data_private_relay {
    type: yesno
    sql: ${TABLE}.user_data_private_relay ;;
  }

  dimension: user_data_prob_cross_platform_ids {
    type: string
    sql: ${TABLE}.user_data_prob_cross_platform_ids ;;
  }

  dimension: user_data_screen_height {
    type: number
    sql: ${TABLE}.user_data_screen_height ;;
  }

  dimension: user_data_screen_width {
    type: number
    sql: ${TABLE}.user_data_screen_width ;;
  }

  dimension: user_data_sdk_version {
    type: string
    sql: ${TABLE}.user_data_sdk_version ;;
  }

  dimension: user_data_user_agent {
    type: string
    sql: ${TABLE}.user_data_user_agent ;;
  }

  dimension_group: uuid_ts {
    type: time
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: event_data_currency {
    type: string
    sql: ${TABLE}.event_data_currency ;;
  }

  dimension: event_data_description {
    type: string
    sql: ${TABLE}.event_data_description ;;
  }

  dimension: event_data_exchange_rate {
    type: number
    sql: ${TABLE}.event_data_exchange_rate ;;
  }

  dimension: event_data_revenue {
    type: number
    sql: ${TABLE}.event_data_revenue ;;
  }

  dimension: event_data_revenue_in_local_currency {
    type: number
    sql: ${TABLE}.event_data_revenue_in_local_currency ;;
  }

  dimension: event_data_revenue_in_usd {
    type: number
    sql: ${TABLE}.event_data_revenue_in_usd ;;
  }

  dimension: event_data_transaction_id {
    type: string
    sql: ${TABLE}.event_data_transaction_id ;;
  }

  dimension: user_data_browser {
    type: string
    sql: ${TABLE}.user_data_browser ;;
  }
}
