view: bigquery_order_completed {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.javascript.order_completed`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: context_locale {
    type: string
    sql: ${TABLE}.context_locale ;;
  }

  dimension: context_page_path {
    type: string
    sql: ${TABLE}.context_page_path ;;
  }

  dimension: context_page_referrer {
    type: string
    sql: ${TABLE}.context_page_referrer ;;
  }

  dimension: context_page_search {
    type: string
    sql: ${TABLE}.context_page_search ;;
  }

  dimension: context_page_title {
    type: string
    sql: ${TABLE}.context_page_title ;;
  }

  dimension: context_page_url {
    type: string
    sql: ${TABLE}.context_page_url ;;
  }

  dimension: context_timezone {
    type: string
    sql: ${TABLE}.context_timezone ;;
  }

  dimension: context_traits_cross_domain_id {
    type: string
    sql: ${TABLE}.context_traits_cross_domain_id ;;
  }

  dimension: context_transaction_campaign {
    type: string
    sql: ${TABLE}.context_transaction_campaign ;;
  }

  dimension: context_transaction_product_id {
    type: number
    sql: ${TABLE}.context_transaction_product_id ;;
  }

  dimension: context_transaction_product_name {
    type: string
    sql: ${TABLE}.context_transaction_product_name ;;
  }

  dimension: context_transaction_product_sku {
    type: string
    sql: ${TABLE}.context_transaction_product_sku ;;
  }

  dimension: context_user_agent {
    type: string
    sql: ${TABLE}.context_user_agent ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: revenue {
    type: number
    sql: ${TABLE}.revenue ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: context_campaign_content {
    type: string
    sql: ${TABLE}.context_campaign_content ;;
  }

  dimension: context_campaign_medium {
    type: string
    sql: ${TABLE}.context_campaign_medium ;;
  }

  dimension: context_campaign_name {
    type: string
    sql: ${TABLE}.context_campaign_name ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
  }

  dimension: context_campaign_term {
    type: string
    sql: ${TABLE}.context_campaign_term ;;
  }

  dimension: collection_id {
    type: number
    sql: ${TABLE}.collection_id ;;
  }

  dimension: context_revenue {
    type: number
    sql: ${TABLE}.context_revenue ;;
  }

  dimension: current_site_id {
    type: number
    sql: ${TABLE}.current_site_id ;;
  }

  dimension: current_site_key {
    type: string
    sql: ${TABLE}.current_site_key ;;
  }

  dimension: current_user_email {
    type: string
    sql: ${TABLE}.current_user_email ;;
  }

  dimension: current_user_id {
    type: number
    sql: ${TABLE}.current_user_id ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: platform_id {
    type: number
    sql: ${TABLE}.platform_id ;;
  }

  dimension: platform_version {
    type: string
    sql: ${TABLE}.platform_version ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  dimension: item_id {
    type: number
    sql: ${TABLE}.item_id ;;
  }

  dimension: context_external_ids {
    type: string
    sql: ${TABLE}.context_external_ids ;;
  }

  dimension: context_traits_email {
    type: string
    sql: ${TABLE}.context_traits_email ;;
  }

  dimension: context_traits_name {
    type: string
    sql: ${TABLE}.context_traits_name ;;
  }

  dimension: context_traits_vimeo_id {
    type: number
    sql: ${TABLE}.context_traits_vimeo_id ;;
  }

  dimension: site_id {
    type: number
    sql: ${TABLE}.site_id ;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.user_email ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      context_app_name,
      context_app_version,
      context_ip,
      context_library_name,
      context_library_version,
      context_locale,
      context_page_path,
      context_page_referrer,
      context_page_search,
      context_page_title,
      context_page_url,
      context_timezone,
      context_traits_cross_domain_id,
      context_transaction_campaign,
      context_transaction_product_id,
      context_transaction_product_name,
      context_transaction_product_sku,
      context_user_agent,
      event,
      event_text,
      id,
      loaded_at_time,
      original_timestamp_time,
      received_at_time,
      revenue,
      sent_at_time,
      timestamp_time,
      user_id,
      uuid_ts_time,
      context_campaign_content,
      context_campaign_medium,
      context_campaign_name,
      context_campaign_source,
      context_campaign_term,
      collection_id,
      context_revenue,
      current_site_id,
      current_site_key,
      current_user_email,
      current_user_id,
      device,
      name,
      platform,
      platform_id,
      platform_version,
      product_id,
      referrer,
      session_id,
      type,
      url,
      video_id,
      view,
      item_id,
      context_external_ids,
      context_traits_email,
      context_traits_name,
      context_traits_vimeo_id,
      site_id,
      user_email
    ]
  }
}
