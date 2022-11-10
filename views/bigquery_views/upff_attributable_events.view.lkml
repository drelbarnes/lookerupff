view: upff_attributable_events {
  derived_table: {
    sql: DECLARE attribution_window INT64 DEFAULT 30 ;
      DECLARE free_trial_window INT64 DEFAULT 15 ;
      -- JOIN ORDERS ON PAGE VISITS
      with web_pages as (
      select
      a.timestamp as viewed_at
      , b.session_start
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
      -- , regexp_extract(referrer, r'[a-zA-Z]+\.[a-zA-Z]+\/') as referrer_domain
      , split(referrer, "?")[safe_offset(1)] AS referrer_query
      , path
      , title as view
      , '' as user_agent
        from `up-faith-and-family-216419.looker_scratch.LR_P4H321668106386112_upff_page_events` a
        left join `up-faith-and-family-216419.looker_scratch.LR_P4YF81668114275665_upff_web_sessions` b
        on a.session_id = b.session_id
      )
      , ios_events as (
        select
        a.timestamp as viewed_at
        , b.session_start
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
        from `up-faith-and-family-216419.looker_scratch.LR_P4GLO1668114279161_ios_app_events` a
        left join `up-faith-and-family-216419.looker_scratch.LR_P4N9K1668114282459_upff_ios_sessions` b
        on a.session_id = b.session_id
      )
      , webhook_events as (
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
        from `up-faith-and-family-216419.looker_scratch.LR_P4IO31668106675324_vimeo_webhook_events`
        where event in ("customer_product_created", "customer_product_free_trial_created", "customer_product_free_trial_converted")
      )
      , order_completed_events as (
        select timestamp as ordered_at
        , user_id as user_id
        , id as event_id
        , anonymous_id
        , device_id
        , context_ip as ip_address
        , user_email as email
        , platform
        from `up-faith-and-family-216419.looker_scratch.LR_P4EFR1668111781660_upff_order_completed_events`
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
          from (select * from order_completed_events where platform = "web") as a
          left join (select * from webhook_events where topic in ("customer_product_created", "customer_product_free_trial_created")) as b
          on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
          left join web_pages as c
          on a.event_id = c.event_id
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
            else b.topic
            end as topic
          from (select * from order_completed_events where platform in ("iphone", "ipad")) as a
          left join (select * from webhook_events where (topic = "customer_product_created" or topic is null) and platform = 'ios') as b
          on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
          left join (select * from webhook_events where (topic = "customer_product_free_trial_created" or topic is null) and platform = 'ios') as c
          on a.user_id = c.user_id and date(a.ordered_at) = date(c.timestamp)
          left join ios.identifies as d
          on a.device_id = d.context_device_id
          left join (select * from webhook_events where email is not null) as e
          on a.email = e.email and date(a.ordered_at) = date(e.timestamp)
      )
      , web_pages_web_orders_anon as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_pages.*
        from web_orders
        full join web_pages
        on web_orders.anonymous_id = web_pages.anonymous_id
        -- and web_orders.ordered_at > web_pages.viewed_at
        -- and web_pages.viewed_at > timestamp_sub(web_orders.ordered_at, INTERVAL 30 DAY)
      )
      , web_pages_web_orders_ip as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_pages.*
        from web_orders
        full join web_pages
        on web_pages.ip_address = web_orders.ip_address
        -- and web_orders.ordered_at > web_pages.viewed_at
        -- and web_pages.viewed_at > timestamp_sub(web_orders.ordered_at, INTERVAL 30 DAY)
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
      , web_pages_ios_orders_ip as (
        select
        ios_orders.ordered_at
        , ios_orders.user_id
        , ios_orders.plan_type
        , ios_orders.platform
        , ios_orders.topic
        , web_pages.*
        from ios_orders
        full join web_pages
        on ios_orders.ip_address = web_pages.ip_address
        -- and ios_orders.ordered_at > web_pages.viewed_at
        -- and web_pages.viewed_at > timestamp_sub(ios_orders.ordered_at, INTERVAL 30 DAY)
      )
      , all_joined_events as (
        select * from web_pages_web_orders_anon
        union all
        select * from web_pages_web_orders_ip
        union all
        select * from ios_events_ios_orders_device
        union all
        select * from ios_events_ios_orders_ip
        union all
        select * from web_pages_ios_orders_ip
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
      row,
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
      source
    ]
  }
}
