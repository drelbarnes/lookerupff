view: bigquery_javascript_firstplay {
  sql_table_name: javascript.firstplay ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_app_build {
    type: number
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

  dimension: context_device_name {
    type: string
    sql: ${TABLE}.context_device_name ;;
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



  dimension: context_locale {
    type: string
    sql: ${TABLE}.context_locale ;;
  }

  dimension: context_network_bluetooth {
    type: yesno
    sql: ${TABLE}.context_network_bluetooth ;;
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

  dimension: context_screen_density {
    type: number
    sql: ${TABLE}.context_screen_density ;;
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

  dimension: context_traits_anonymous_id {
    type: string
    sql: ${TABLE}.context_traits_anonymous_id ;;
  }

  dimension: context_traits_cross_domain_id {
    type: string
    sql: ${TABLE}.context_traits_cross_domain_id ;;
  }

  dimension: context_traits_user_id {
    type: string
    sql: ${TABLE}.context_traits_user_id ;;
  }

  dimension: context_user_agent {
    type: string
    sql: ${TABLE}.context_user_agent ;;
  }


  dimension: current_src {
    type: string
    sql: ${TABLE}.current_src ;;
  }

  dimension: current_subtitle {
    type: string
    sql: ${TABLE}.current_subtitle ;;
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

  dimension_group: loaded {
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
    sql: ${TABLE}.loaded_at ;;
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
    type: number
    sql: ${TABLE}.platform_id ;;
  }

  dimension: platform_version {
    type: string
    sql: ${TABLE}.platform_version ;;
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



  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
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


  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
    sql:  ${user_id} ;;
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      context_campaign_name,
      context_library_name,
      context_library_name,
      name,
      context_app_name,
      context_device_name,
      context_os_name
    ]
  }
}
