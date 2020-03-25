view: bigquery_firebase_events {

  derived_table: {
    sql: SELECT * FROM `up-faith-and-family.analytics_164012552.events_*`
      WHERE
        _TABLE_SUFFIX = CAST(FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ) AS STRING) ;;

    }

    dimension: app_info {
      hidden: yes
      sql: ${TABLE}.app_info ;;
    }

    dimension: device {
      hidden: yes
      sql: ${TABLE}.device ;;
    }

    dimension: device_category {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.category') ;;
    }

    dimension: device_mobile_brand_name {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.mobile_brand_name') ;;
    }

    dimension: device_mobile_model_name {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.mobile_model_name') ;;
    }

    dimension: device_mobile_marketing_name {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.mobile_marketing_name') ;;
    }

    dimension: device_mobile_os_hardware_model {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.mobile_os_hardware_model') ;;
    }

    dimension: device_operating_system {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.operating_system') ;;
    }

    dimension: device_operating_system_version {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.operating_system_version') ;;
    }

    dimension: device_advertising_id {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.device), '$.advertising_id') ;;
    }

    dimension: event_bundle_sequence_id {
      type: number
      sql: ${TABLE}.event_bundle_sequence_id ;;
    }

    dimension: event_date {
      type: string
      sql: ${TABLE}.event_date ;;
    }

    dimension: event_dimensions {
      hidden: yes
      sql: ${TABLE}.event_dimensions ;;
    }

    dimension: event_name {
      type: string
      sql: ${TABLE}.event_name ;;
    }

    dimension: event_params {
      hidden: yes
      sql: ${TABLE}.event_params ;;
    }

    dimension: event_params_key {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.event_params), '$') ;;
    }

    dimension: event_previous_timestamp {
      type: number
      sql: ${TABLE}.event_previous_timestamp ;;
    }

    dimension: event_server_timestamp_offset {
      type: number
      sql: ${TABLE}.event_server_timestamp_offset ;;
    }

    dimension: event_timestamp {
      type: number
      sql: ${TABLE}.event_timestamp ;;
    }

    dimension: event_value_in_usd {
      type: number
      sql: ${TABLE}.event_value_in_usd ;;
    }

    dimension: geo {
      hidden: yes
      sql: ${TABLE}.geo ;;
    }

    dimension: geo_continent {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.geo), '$.continent') ;;
    }

    dimension: geo_country {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.geo), '$.country') ;;
    }

    dimension: geo_region {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.geo), '$.region') ;;
    }

    dimension: geo_city {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.geo), '$.city') ;;
    }

    dimension: geo_sub_continent {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.geo), '$.sub_continent') ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: stream_id {
      type: string
      sql: ${TABLE}.stream_id ;;
    }

    dimension: traffic_source {
      hidden: yes
      sql: ${TABLE}.traffic_source ;;
    }

    dimension: traffic_source_name {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.traffic_source), '$.name') ;;
    }

    dimension: traffic_source_medium {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.traffic_source), '$.medium') ;;
    }

    dimension: traffic_source_source {
      type: string
      sql: JSON_EXTRACT_SCALAR(TO_JSON_STRING(${TABLE}.traffic_source), '$.source') ;;
    }

    dimension: user_first_touch_timestamp {
      type: number
      sql: ${TABLE}.user_first_touch_timestamp ;;
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    measure: count {
      type: count
      drill_fields: [event_name]
    }

  }
