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
