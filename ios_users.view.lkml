view: ios_users {
  sql_table_name: ios.users ;;

  dimension: id {
    primary_key: yes
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.id ;;
  }

  dimension: context_app_build {
    type: string
    sql: ${TABLE}.context_app_build ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension: context_app_namespace {
    type: string
    sql: ${TABLE}.context_app_namespace ;;
  }

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_device_ad_tracking_enabled {
    type: yesno
    sql: ${TABLE}.context_device_ad_tracking_enabled ;;
  }

  dimension: context_device_advertising_id {
    type: string
    sql: ${TABLE}.context_device_advertising_id ;;
  }

  dimension: context_device_id {
    type: string
    sql: ${TABLE}.context_device_id ;;
  }

  dimension: context_device_manufacturer {
    type: string
    sql: ${TABLE}.context_device_manufacturer ;;
  }

  dimension: context_device_model {
    type: string
    sql: ${TABLE}.context_device_model ;;
  }

  dimension: context_device_token {
    type: string
    sql: ${TABLE}.context_device_token ;;
  }

  dimension: context_device_type {
    type: string
    sql: ${TABLE}.context_device_type ;;
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

  dimension: context_network_carrier {
    type: string
    sql: ${TABLE}.context_network_carrier ;;
  }

  dimension: context_network_cellular {
    type: yesno
    sql: ${TABLE}.context_network_cellular ;;
  }

  dimension: context_network_wifi {
    type: yesno
    sql: ${TABLE}.context_network_wifi ;;
  }

  dimension: context_os_name {
    type: string
    sql: ${TABLE}.context_os_name ;;
  }

  dimension: context_os_version {
    type: string
    sql: ${TABLE}.context_os_version ;;
  }

  dimension: context_screen_height {
    type: number
    sql: ${TABLE}.context_screen_height ;;
  }

  dimension: context_screen_width {
    type: number
    sql: ${TABLE}.context_screen_width ;;
  }

  dimension: context_timezone {
    type: string
    sql: ${TABLE}.context_timezone ;;
  }

  dimension: context_traits_email {
    type: string
    sql: ${TABLE}.context_traits_email ;;
  }

  dimension: context_traits_name {
    type: string
    sql: ${TABLE}.context_traits_name ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      context_app_name,
      context_library_name,
      context_os_name,
      context_traits_name,
      name
    ]
  }
}
