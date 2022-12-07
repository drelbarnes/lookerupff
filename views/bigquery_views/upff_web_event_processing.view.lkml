view: upff_web_event_processing {
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
      , b.session_search as referrer_query
      , b.ad_id
      , path
      , title as view
      , '' as user_agent
        from ${upff_page_events.SQL_TABLE_NAME} as a
        left join ${upff_web_sessions.SQL_TABLE_NAME} as b
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
          and platform = "web"
          and user_id is not null
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
          where platform in ("web")
          and anonymous_id is not null
        )
        , p1 as (
          select *
          , row_number() over (partition by anonymous_id, date(ordered_at)) as n
          from p0
        )
        select ordered_at, user_id, event_id, anonymous_id, device_id, ip_address, email, platform
        from p1
        where n = 1
      )
      , web_orders as (
          select
          a.ordered_at
          , a.user_id
          -- hotfix for the anonymous_id bug of Q2/Q3 2022
          , a.anonymous_id as anonymous_id_raw
          , c.anonymous_id as anonymous_id
          , a.device_id
          , a.event_id
          , a.ip_address
          , a.platform
          , b.plan_type
          , b.topic
          from order_completed_events as a
          left join webhook_events as b
          on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
          left join web_events as c
          on a.event_id = c.event_id
      )
      , web_events_web_orders_anon as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_events.*
        from web_orders
        full join web_events
        on web_orders.anonymous_id = web_events.anonymous_id
        -- and web_orders.ordered_at > web_events.viewed_at
        -- and web_events.viewed_at > timestamp_sub(web_orders.ordered_at, INTERVAL 30 DAY)
      )
      , web_events_web_orders_ip as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_events.*
        from web_orders
        full join web_events
        on web_events.ip_address = web_orders.ip_address
        -- and web_orders.ordered_at > web_events.viewed_at
        -- and web_events.viewed_at > timestamp_sub(web_orders.ordered_at, INTERVAL 30 DAY)
      )
      , all_joined_events as (
        select * from web_events_web_orders_anon
        union all
        select * from web_events_web_orders_ip
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
        , ad_id
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
        when (sum(attribution_flag) over (partition by user_id) = 0) then "unknown"
        when sum(attribution_flag) over (partition by user_id) > 0 and utm_source is null and referrer_domain != "upfaithandfamily.com/" then referrer_domain
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
        , session_id
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
        , ad_id
        , referrer_domain
        , user_agent
        , source
        from final_p1
        where source is not null
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
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

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
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

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
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
      session_id,
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
      ad_id,
      referrer_domain,
      user_agent,
      source,
      row
    ]
  }
}
