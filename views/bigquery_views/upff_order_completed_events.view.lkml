view: upff_order_completed_events {
  derived_table: {
    sql:
      select anonymous_id, context_app_name, context_app_version, context_external_ids, context_library_name, context_library_version, context_traits_email, context_traits_name, cast(context_traits_vimeo_id as string) as context_traits_vimeo_id, cast(context_transaction_product_id as string) as context_transaction_product_id, context_user_agent, cast(null as string) as conversion_type, device, cast(null as string) as device_id, event, event_text, id, loaded_at, name, original_timestamp, platform, cast(platform_id as string) as platform_id, platform_version, product_id, received_at, sent_at, session_id, site_id, timestamp, type, user_email, cast(user_id as string) as user_id, uuid_ts, view from javascript.order_completed
      union all
      select anonymous_id, context_app_name, context_app_version, context_external_ids, context_library_name, context_library_version, context_traits_email, context_traits_name, cast(context_traits_vimeo_id as string) as context_traits_vimeo_id, cast(context_transaction_product_id as string) as context_transaction_product_id, context_user_agent, conversion_type, device, device_id, event, event_text, id, loaded_at, name, original_timestamp, platform, cast(platform_id as string) as platform_id, platform_version, product_id, received_at, sent_at, session_id, site_id, timestamp, type, user_email, cast(user_id as string) as user_id, uuid_ts, view from ios.order_completed
      union all
      select anonymous_id, context_app_name, context_app_version, context_external_ids, context_library_name, context_library_version, context_traits_email, context_traits_name, cast(context_traits_vimeo_id as string) as context_traits_vimeo_id, cast(context_transaction_product_id as string) as context_transaction_product_id, context_user_agent, conversion_type, device, device_id, event, event_text, id, loaded_at, name, original_timestamp, platform, cast(platform_id as string) as platform_id, platform_version, product_id, received_at, sent_at, session_id, site_id, timestamp, type, user_email, cast(user_id as string) as user_id, uuid_ts, view from android.order_completed
      union all
      select anonymous_id, context_app_name, context_app_version, context_external_ids, context_library_name, context_library_version, context_traits_email, context_traits_name, cast(context_traits_vimeo_id as string) as context_traits_vimeo_id, cast(context_transaction_product_id as string) as context_transaction_product_id, context_user_agent, conversion_type, device, device_id, event, event_text, id, loaded_at, name, original_timestamp, platform, cast(platform_id as string) as platform_id, platform_version, product_id, received_at, sent_at, session_id, site_id, timestamp, type, user_email, cast(user_id as string) as user_id, uuid_ts, view from roku.order_completed
      union all
      select anonymous_id, context_app_name, context_app_version, context_external_ids, context_library_name, context_library_version, context_traits_email, context_traits_name, cast(context_traits_vimeo_id as string) as context_traits_vimeo_id, cast(context_transaction_product_id as string) as context_transaction_product_id, context_user_agent, cast(null as string) as conversion_type, device, device_id, event, event_text, id, loaded_at, name, original_timestamp, platform, cast(platform_id as string) as platform_id, platform_version, product_id, received_at, sent_at, session_id, site_id, timestamp, type, user_email, cast(user_id as string) as user_id, uuid_ts, view from amazon_fire_tv.order_completed
       ;;
    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
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

  dimension: context_external_ids {
    type: string
    sql: ${TABLE}.context_external_ids ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
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
    type: string
    sql: ${TABLE}.context_traits_vimeo_id ;;
  }

  dimension: context_transaction_product_id {
    type: string
    sql: ${TABLE}.context_transaction_product_id ;;
  }

  dimension: context_user_agent {
    type: string
    sql: ${TABLE}.context_user_agent ;;
  }

  dimension: conversion_type {
    type: string
    sql: ${TABLE}.conversion_type ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
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

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: platform_id {
    type: string
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

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: site_id {
    type: number
    sql: ${TABLE}.site_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.user_email ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      context_app_name,
      context_app_version,
      context_external_ids,
      context_library_name,
      context_library_version,
      context_traits_email,
      context_traits_name,
      context_traits_vimeo_id,
      context_transaction_product_id,
      context_user_agent,
      conversion_type,
      device,
      device_id,
      event,
      event_text,
      id,
      loaded_at_time,
      name,
      original_timestamp_time,
      platform,
      platform_id,
      platform_version,
      product_id,
      received_at_time,
      sent_at_time,
      session_id,
      site_id,
      timestamp_time,
      type,
      user_email,
      user_id,
      uuid_ts_time,
      view
    ]
  }
}
