view: vimeo_app_screens {
  derived_table: {
    sql: with roku_screens as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , "" as ip_address
        , "" as cross_domain_id
        , safe_cast(device_id as string) as device_id
        , 'screen_view' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(view as string) as view
        , "" as url
        , "" as path
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from roku.screens
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , ios_screens as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , "" as cross_domain_id
        , safe_cast(device_id as string) as device_id
        , 'screen_view' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(view as string) as view
        , "" as url
        , "" as path
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from ios.screens
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , android_screens as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , "" as cross_domain_id
        , safe_cast(device_id as string) as device_id
        , 'screen_view' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(view as string) as view
        , "" as url
        , "" as path
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from android.screens
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , amazon_fire_tv_screens as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , "" as cross_domain_id
        , safe_cast(device_id as string) as device_id
        , 'screen_view' as event
        , "" as utm_content
        , "" as utm_medium
        , "" as utm_campaign
        , "" as utm_source
        , "" as utm_term
        , "" as referrer
        , safe_cast(view as string) as view
        , "" as url
        , "" as path
        , safe_cast(platform as string) as device
        , safe_cast(session_id as string) as session_id
        , timestamp
        from amazon_fire_tv.screens
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        -- timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , union_all as (
      SELECT * FROM roku_screens
      UNION ALL
      SELECT * FROM ios_screens
      UNION ALL
      SELECT * FROM android_screens
      UNION ALL
      SELECT * FROM amazon_fire_tv_screens
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

  dimension: cross_domain_id {
    type: string
    sql: ${TABLE}.cross_domain_id ;;
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

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: path {
    type: string
    sql: ${TABLE}.path ;;
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
      cross_domain_id,
      device_id,
      event,
      utm_content,
      utm_medium,
      utm_campaign,
      utm_source,
      utm_term,
      referrer,
      view,
      url,
      path,
      device,
      session_id,
      timestamp_time
    ]
  }
}
