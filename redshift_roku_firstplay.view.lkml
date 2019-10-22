view: redshift_roku_firstplay {
  sql_table_name: roku.firstplay ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: current_src {
    type: string
    sql: ${TABLE}.current_src ;;
  }

  dimension: current_type {
    type: string
    sql: ${TABLE}.current_type ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: is_airplay {
    type: number
    sql: ${TABLE}.is_airplay ;;
  }

  dimension: is_chromecast {
    type: number
    sql: ${TABLE}.is_chromecast ;;
  }

  dimension: is_drm {
    type: number
    sql: ${TABLE}.is_drm ;;
  }

  dimension: is_fullscreen {
    type: number
    sql: ${TABLE}.is_fullscreen ;;
  }

  dimension: is_live {
    type: number
    sql: ${TABLE}.is_live ;;
  }

  dimension: is_trailer {
    type: number
    sql: ${TABLE}.is_trailer ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: original_timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
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
    type: string
    sql: ${TABLE}.product_id ;;
  }

  dimension_group: received {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.received_at ;;
  }

  dimension: seconds {
    type: number
    sql: ${TABLE}.seconds ;;
  }

  dimension_group: sent {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.sent_at ;;
  }

  dimension: timecode {
    type: number
    sql: ${TABLE}.timecode ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, context_library_name]
  }
}
