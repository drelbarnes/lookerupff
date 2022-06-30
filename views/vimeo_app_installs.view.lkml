view: vimeo_app_installs {
  derived_table: {
    sql: with roku_app_installed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , "" as ip_address
        , safe_cast(device_id as string) as device_id
        , 'app_install' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from roku.app_installed
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , ios_app_installed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        , 'app_install' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from ios.app_installed
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , android_app_installed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        , 'app_install' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from android.app_installed
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , amazon_fire_tv_app_installed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(device_id as string) as device_id
        , 'app_install' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from amazon_fire_tv.app_installed
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , union_all as (
      SELECT * FROM roku_app_installed
      UNION ALL
      SELECT * FROM ios_app_installed
      UNION ALL
      SELECT * FROM android_app_installed
      UNION ALL
      SELECT * FROM amazon_fire_tv_app_installed
      )
      select * FROM union_all
       ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  parameter: attribution_window {
    type: unquoted
    label: "Attribution Window"
    allowed_value: {
      label: "7 days"
      value: "7"
    }
    allowed_value: {
      label: "14 days"
      value: "14"
    }
    allowed_value: {
      label: "28 days"
      value: "28"
    }
    allowed_value: {
      label: "30 days"
      value: "30"
    }
    allowed_value: {
      label: "60 days"
      value: "60"
    }
    allowed_value: {
      label: "90 days"
      value: "90"
    }
  }

  parameter: order_window {
    type: unquoted
    label: "Order Completed Window"
    allowed_value: {
      label: "7 days"
      value: "7"
    }
    allowed_value: {
      label: "14 days"
      value: "14"
    }
    allowed_value: {
      label: "28 days"
      value: "28"
    }
    allowed_value: {
      label: "30 days"
      value: "30"
    }
    allowed_value: {
      label: "60 days"
      value: "60"
    }
    allowed_value: {
      label: "90 days"
      value: "90"
    }

    allowed_value: {
      label: "180 days"
      value: "180"
    }

    allowed_value: {
      label: "365 days"
      value: "365"
    }
  }

  measure: distinct_count_device_id {
    type: count_distinct
    sql:${device_id} ;;
    drill_fields: [detail*]
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  set: detail {
    fields: [
      user_id,
      anonymous_id,
      ip_address,
      device_id,
      event,
      utm_content,
      utm_medium,
      utm_campaign,
      utm_source,
      utm_term,
      referrer,
      device,
      session_id,
      timestamp_time
    ]
  }
}
