view: upff_ios_event_processing {
  derived_table: {
    sql:
      -- JOIN ORDERS ON PAGE VISITS
      with web_events as (
      select
      a.timestamp as viewed_at
      , coalesce(b.session_start, a.timestamp) as session_start
      , a.event_id
      , a.anonymous_id
      , cast(null as string) as device_id
      , a.session_id
      , a.ip_address
      , b.first_utm_content as utm_content
      , b.first_utm_medium as utm_medium
      , b.first_utm_campaign as utm_campaign
      , b.first_utm_source as utm_source
      , b.first_utm_term as utm_term
      , b.session_referrer as referrer_domain
      , split(referrer, "?")[safe_offset(1)] AS referrer_query
      , path
      , title as view
      , '' as user_agent
        from ${upff_page_events.SQL_TABLE_NAME} as a
        left join ${upff_web_sessions.SQL_TABLE_NAME} as b
        on a.session_id = b.session_id
      )
      , ios_events as (
        select
        a.timestamp as viewed_at
        , coalesce(b.session_start, a.timestamp) as session_start
        , a.id as event_id
        , a.anonymous_id
        , a.device_id
        , a.session_id
        , a.ip_address
        , b.first_utm_content as utm_content
        , b.first_utm_medium as utm_medium
        , b.first_utm_campaign as utm_campaign
        , b.first_utm_source as utm_source
        , b.first_utm_term as utm_term
        , cast(null as string) as referrer_domain
        , cast(null as string) as referrer_query
        , cast(null as string) as path
        , cast(null as string) as view
        , cast(null as string) as user_agent
        from ${ios_app_events.SQL_TABLE_NAME} as a
        left join ${upff_ios_sessions.SQL_TABLE_NAME} as b
        on a.session_id = b.session_id
      )
      , webhook_events as (
        with p0 as (
          select
          timestamp
          , user_id
          , email
          , event as topic
          , case
            when subscription_frequency in (null, "custom", "monthly") then "monthly"
            else "yearly"
            end as plan_type
          , platform
          from ${vimeo_webhook_events.SQL_TABLE_NAME}
          where event in ("customer_product_created", "customer_product_free_trial_created")
          and platform = "ios"
        )
        , p1 as (
          select *
          , row_number() over (partition by user_id, date(timestamp)) as n
          from p0
        )
        select timestamp, user_id, email, topic, plan_type, platform
        from p1
        where n = 1
      )
      , order_completed_events as (
        with p0 as (
          select timestamp as ordered_at
          , user_id as user_id
          , id as event_id
          , anonymous_id
          , device_id
          , context_ip as ip_address
          , user_email as email
          , platform
          from ${upff_order_completed_events.SQL_TABLE_NAME}
          where platform in ("iphone", "ipad")
        )
        , p1 as (
          select *
          , row_number() over (partition by device_id, date(ordered_at)) as n
          from p0
        )
        select ordered_at, user_id, event_id, anonymous_id, device_id, ip_address, email, platform
        from p1
        where n = 1
      )
      , ios_orders as (
          select
          a.ordered_at
          , coalesce(a.user_id, d.user_id, e.user_id, a.device_id) as user_id
          , a.anonymous_id
          -- hotfix for the anonymous_id bug of Q2/Q3 2022
          , cast(null as string) as anonymous_id_raw
          , a.device_id
          , a.event_id
          , a.ip_address
          , a.platform
          , b.plan_type
          , case
            when b.topic = "customer_product_created" and c.topic = "customer_product_free_trial_created" then c.topic
            when b.topic is null and c.topic is null then "customer_product_free_trial_created"
            else b.topic
            end as topic
          from order_completed_events as a
          left join (select * from webhook_events where topic = "customer_product_created" or topic is null) as b
          on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
          left join (select * from webhook_events where topic = "customer_product_free_trial_created" or topic is null) as c
          on a.user_id = c.user_id and date(a.ordered_at) = date(c.timestamp)
          left join ios.identifies as d
          on a.device_id = d.context_device_id
          left join (select * from webhook_events where email is not null) as e
          on a.email = e.email and date(a.ordered_at) = date(e.timestamp)
      )
      , ios_events_ios_orders_device as (
        select
        ios_orders.ordered_at
        , ios_orders.user_id
        , ios_orders.plan_type
        , ios_orders.platform
        , ios_orders.topic
        , ios_events.*
        from ios_orders
        full join ios_events
        on ios_orders.device_id = ios_events.device_id
        -- and ios_orders.ordered_at > ios_events.viewed_at
        -- and ios_events.viewed_at > timestamp_sub(ios_orders.ordered_at, INTERVAL 30 DAY)
      )
      , ios_events_ios_orders_ip as (
        select
        ios_orders.ordered_at
        , ios_orders.user_id
        , ios_orders.plan_type
        , ios_orders.platform
        , ios_orders.topic
        , ios_events.*
        from ios_orders
        full join ios_events
        on ios_orders.ip_address = ios_events.ip_address
        -- and ios_orders.ordered_at > ios_events.viewed_at
        -- and ios_events.viewed_at > timestamp_sub(ios_orders.ordered_at, INTERVAL 30 DAY)
      )
      , web_events_ios_orders_ip as (
        select
        ios_orders.ordered_at
        , ios_orders.user_id
        , ios_orders.plan_type
        , ios_orders.platform
        , ios_orders.topic
        , web_events.*
        from ios_orders
        full join web_events
        on ios_orders.ip_address = web_events.ip_address
        -- and ios_orders.ordered_at > web_events.viewed_at
        -- and web_events.viewed_at > timestamp_sub(ios_orders.ordered_at, INTERVAL 30 DAY)
      )
      , all_joined_events as (
        select * from ios_events_ios_orders_device
        union all
        select * from ios_events_ios_orders_ip
        union all
        select * from web_events_ios_orders_ip
      )
      , final_p0 as (
        select
        ordered_at
        , viewed_at
        , session_start
        , user_id
        , anonymous_id
        , device_id
        , session_id
        , ip_address
        , plan_type
        , platform
        , topic
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , path
        , view
        , referrer_domain
        , referrer_query
        , user_agent
        , case
          when utm_source is null and (referrer_domain is null or referrer_domain = "upfaithandfamily.com/") then 0
          else 1
          end as attribution_flag
        from all_joined_events
        where viewed_at is not null
        and viewed_at < ordered_at
        and viewed_at >= timestamp_sub(ordered_at, INTERVAL 30 DAY)
      )
      , final_p1 as (
        select *
        , case
        when (sum(attribution_flag) over (partition by device_id) = 0) then "unknown"
        when sum(attribution_flag) over (partition by device_id) > 0 and utm_source is null and referrer_domain != "upfaithandfamily.com/" then referrer_domain
        else utm_source
        end as source
        from final_p0
      )
      , attributable_events as (
        select
        ordered_at
        , session_start
        , user_id
        , anonymous_id
        , device_id
        , ip_address
        , plan_type
        , platform
        , topic
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , referrer_domain
        , user_agent
        , source
        from final_p1
        where source is not null
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
      )
      select *, row_number() over (order by ordered_at) as row from attributable_events
       ;;
    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  dimension_group: ordered_at {
    type: time
    sql: ${TABLE}.ordered_at ;;
  }

  dimension_group: session_start {
    type: time
    sql: ${TABLE}.session_start ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: plan_type {
    type: string
    sql: ${TABLE}.plan_type ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
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

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  set: detail {
    fields: [
      ordered_at_time,
      session_start_time,
      user_id,
      anonymous_id,
      device_id,
      ip_address,
      plan_type,
      platform,
      topic,
      utm_content,
      utm_medium,
      utm_campaign,
      utm_source,
      utm_term,
      referrer_domain,
      user_agent,
      source,
      row
    ]
  }
}
