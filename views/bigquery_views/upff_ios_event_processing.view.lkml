view: upff_ios_event_processing {
  derived_table: {
    sql: -- JOIN ORDERS ON PAGE VISITS
      with web_events as (
        select
        session_start
        , session_id
        , event_id
        , anonymous_id
        , cast(null as string) as device_id
        , cast(null as string) as advertising_id
        , ip_address
        , user_agent
        , session_referrer as referrer_domain
        , session_search as referrer_search
        , session_ad_id as ad_id
        , session_adset_id as adset_id
        , session_campaign_id as campaign_id
        , session_utm_content as utm_content
        , session_utm_medium as utm_medium
        , session_utm_campaign as utm_campaign
        , session_utm_source as utm_source
        , session_utm_term as utm_term
        , landing_page
          from ${upff_web_sessions.SQL_TABLE_NAME}
      )
      , ios_events as (
        select
        session_start
        , session_id
        , safe_cast(event_id as string) as event_id
        , anonymous_id
        , device_id
        , advertising_id
        , ip_address
        , user_agent
        , session_referrer as referrer_domain
        , session_search as referrer_search
        , session_ad_id as ad_id
        , session_adset_id as adset_id
        , session_campaign_id as campaign_id
        , session_utm_content as utm_content
        , session_utm_medium as utm_medium
        , session_utm_campaign as utm_campaign
        , session_utm_source as utm_source
        , session_utm_term as utm_term
        , cast(null as string) as landing_page
        from ${upff_ios_sessions.SQL_TABLE_NAME}
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
          and user_id is not null
        )
        , p1 as (
          select *
          , row_number() over (partition by user_id, date(timestamp)) as n
          from p0
        )
        -- Because of the flow of events for non-web platforms, we cannot just grab the most recent topic.
        select timestamp, user_id, email, topic, plan_type, platform
        from p0
        -- where n = 1
      )
      , order_completed_events as (
        with p0 as (
          select timestamp as ordered_at
          , user_id as user_id
          , to_hex(sha1(concat(id,safe_cast(timestamp as string)))) as event_id
          , anonymous_id
          , device_id
          , context_ip as ip_address
          , context_user_agent as user_agent
          , user_email as email
          , platform
          from ${upff_order_completed_events.SQL_TABLE_NAME}
          where platform in ("iphone", "ipad")
          and device_id is not null
        )
        , p1 as (
          select *
          , row_number() over (partition by device_id, date(ordered_at)) as n
          from p0
        )
        select ordered_at, user_id, event_id, anonymous_id, device_id, ip_address, email, platform, user_agent
        from p1
        where n = 1
      )
      , ios_orders as (
        with p0 as (
          select
          a.ordered_at
          , case
              when a.user_id is not null and a.user_id != "0" then a.user_id
              when (a.user_id is null or a.user_id = "0") and (b.user_id is not null and b.user_id != "0") then b.user_id
              else a.device_id
              end as user_id
          , a.anonymous_id
          , cast(null as string) as anonymous_id_raw
          , a.device_id
          , a.event_id
          , a.ip_address
          , a.user_agent
          , a.email
          , a.platform
          from order_completed_events as a
          left join (select * from ios.identifies where user_id is not null and user_id != "0") as b
          on a.anonymous_id = b.anonymous_id and a.device_id = b.context_device_id
        )
        , p1 as (
          select a.*
          , b.plan_type
          -- this section accounts for the flow of topics for non-web platforms
          , case
            when b.topic = "customer_product_created" and c.topic = "customer_product_free_trial_created" then c.topic
            when b.topic is null and c.topic is null then "customer_product_free_trial_created"
            else b.topic
            end as topic
          from p0 as a
          left join (select * from webhook_events where topic = "customer_product_created" or topic is null) as b
          on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
          left join (select * from webhook_events where topic = "customer_product_free_trial_created" or topic is null) as c
          on a.user_id = c.user_id and date(a.ordered_at) = date(c.timestamp)
        )
        select
        ordered_at, user_id, anonymous_id, device_id, event_id, ip_address, user_agent, platform, plan_type, topic
        from p1
        group by 1,2,3,4,5,6,7,8,9,10
      )
      , ios_events_ios_orders_device as (
        select
        ios_orders.ordered_at
        , ios_orders.user_id
        , ios_orders.plan_type
        , ios_orders.platform
        , ios_orders.topic
        , ios_events.*
        , to_hex(sha1(concat(safe_cast(ios_events.ip_address as string),safe_cast(ios_events.user_agent as string)))) as user_agent_id
        from ios_orders
        full join ios_events
        on ios_orders.device_id = ios_events.device_id
      )
      , ios_events_ios_orders_ip as (
        with p0 as (
          select
          ios_orders.ordered_at
          , ios_orders.user_id
          , ios_orders.plan_type
          , ios_orders.platform
          , ios_orders.topic
          , ios_events.*
          , to_hex(sha1(concat(safe_cast(ios_events.ip_address as string),safe_cast(ios_events.user_agent as string)))) as user_agent_id
          from (select * from ios_orders where ip_address is not null) as ios_orders
          full join (select * from ios_events where ip_address is not null) as ios_events
          on ios_orders.ip_address = ios_events.ip_address
          --and ios_orders.user_agent = ios_events.user_agent
        )
        , p1 as (
          select
          ip_address
          , count(distinct user_id) as n
          from p0
          group by 1 having n > 1
        )
        , p2 as (
          select user_id
          from p0
          where ip_address in (select ip_address from p1)
          group by user_id
        )
        select *
        from p0
        where user_id not in (select user_id from p2)
      )
      , web_events_ios_orders_ip as (
        with p0 as (
          select
          ios_orders.ordered_at
          , ios_orders.user_id
          , ios_orders.plan_type
          , ios_orders.platform
          , ios_orders.topic
          , web_events.*
          , to_hex(sha1(concat(safe_cast(web_events.ip_address as string),safe_cast(web_events.user_agent as string)))) as user_agent_id
          from (select * from ios_orders where ip_address is not null) as ios_orders
          full join (select * from web_events where ip_address is not null) as web_events
          on ios_orders.ip_address = web_events.ip_address
        )
        , p1 as (
          select
          ip_address
          , count(distinct user_id) as n
          from p0
          group by 1 having n > 1
        )
        , p2 as (
          select user_id
          from p0
          where ip_address in (select ip_address from p1)
          group by user_id
        )
        select *
        from p0
        where user_id not in (select user_id from p2)
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
        , session_start
        , user_id
        , anonymous_id
        , device_id
        , advertising_id
        , session_id
        , event_id
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
        , adset_id
        , campaign_id
        , landing_page
        , referrer_domain
        , referrer_search
        , user_agent
        , case
          when utm_source is null and (referrer_domain is null or referrer_domain = "upfaithandfamily.com/") then 0
          else 1
          end as attribution_flag
        from all_joined_events
        where session_start is not null
        and session_start < ordered_at
        and session_start >= timestamp_sub(ordered_at, INTERVAL 30 DAY)
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
        , session_id
        , event_id
        , user_id
        , anonymous_id
        , device_id
        , advertising_id
        , ip_address
        , user_agent
        , plan_type
        , platform
        , topic
        , ad_id
        , adset_id
        , campaign_id
        , referrer_domain
        , referrer_search
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , source
        from final_p1
        where source is not null
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
      )
      select *, row_number() over (order by ordered_at) as row from attributable_events;;
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

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
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

  dimension: advertising_id {
    type: string
    sql: ${TABLE}.advertising_id ;;
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

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: referrer_search {
    type: string
    sql: ${TABLE}.referrer_search ;;
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
      session_id,
      event_id,
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
      ad_id,
      adset_id,
      campaign_id,
      referrer_domain,
      referrer_search,
      user_agent,
      source,
      row
    ]
  }
}
