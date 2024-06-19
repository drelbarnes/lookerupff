view: upff_web_event_processing {
  derived_table: {
    sql: with web_events as (
      select
      session_start
      , session_id
      , event_id
      , anonymous_id
      , cast(null as string) as device_id
      , cast(null as string) as advertising_id
      , ip_address
      , session_utm_content as utm_content
      , session_utm_medium as utm_medium
      , session_utm_campaign as utm_campaign
      , session_utm_source as utm_source
      , session_utm_term as utm_term
      , session_referrer as referrer_domain
      , session_search as referrer_search
      , session_ad_id as ad_id
      , session_adset_id as adset_id
      , session_campaign_id as campaign_id
      , landing_page
      , user_agent
      from ${upff_web_sessions.SQL_TABLE_NAME}
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
        , promotion_code
        from ${upff_webhook_events.SQL_TABLE_NAME}
        where event in ("customer_product_created", "customer_product_free_trial_created")
        and platform = "web"
        and user_id is not null
      )
      , p1 as (
        select *
        , row_number() over (partition by user_id, date(timestamp)) as n
        from p0
      )
      select timestamp, user_id, email, topic, plan_type, platform, promotion_code
      from p1
      where n = 1
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
        where platform in ("web")
        and anonymous_id is not null
      )
      , p1 as (
        select *
        , row_number() over (partition by anonymous_id, date(ordered_at)) as n
        from p0
      )
      select
      ordered_at
      , user_id
      , event_id
      , anonymous_id
      , device_id
      , ip_address
      , user_agent
      , email
      , platform
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
      , a.user_agent
      , a.ip_address
      , a.platform
      , b.plan_type
      , b.topic
      , b.promotion_code
      from order_completed_events as a
      left join webhook_events as b
      on a.user_id = b.user_id and date(a.ordered_at) = date(b.timestamp)
      left join (
        select anonymous_id, event_id
        from ${upff_page_events.SQL_TABLE_NAME}
      ) as c
      on a.event_id = c.event_id
    )
    , web_events_web_orders_anon as (
      select
      web_orders.ordered_at
      , web_orders.user_id
      , web_orders.plan_type
      , web_orders.platform
      , web_orders.topic
      , web_orders.promotion_code
      , web_events.*
      , to_hex(sha1(concat(safe_cast(web_events.ip_address as string),safe_cast(web_events.user_agent as string)))) as user_agent_id
      from web_orders
      full join web_events
      on web_orders.anonymous_id = web_events.anonymous_id
    )
    , web_events_web_orders_ip as (
      with p0 as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_orders.promotion_code
        , web_events.*
        , to_hex(sha1(concat(safe_cast(web_events.ip_address as string),safe_cast(web_events.user_agent as string)))) as user_agent_id
        from web_orders
        full join web_events
        on web_events.ip_address = web_orders.ip_address
        and web_events.user_agent = web_orders.user_agent
      )
      , p1 as (
        select
        user_agent_id
        , count(distinct user_id) as n
        from p0
        group by 1 having n > 1
      )
      , p2 as (
        select user_id
        from p0
        where user_agent_id in (select user_agent_id from p1)
        group by user_id
      )
      select *
      from p0
      where user_id not in (select user_id from p2)
    )
    , all_joined_events as (
      select * from web_events_web_orders_anon
      union all
      select * from web_events_web_orders_ip
    )
    , final_p0 as (
      select
      ordered_at
      , session_start
      , user_id
      , anonymous_id
      , event_id
      , device_id
      , advertising_id
      , session_id
      , ip_address
      , user_agent
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
      , promotion_code
      , case
        when utm_source is null and (referrer_domain is null or referrer_domain in ("upfaithandfamily.com/", "upfaithandfamily.com", "vhx.tv")) then 0
        else 1
        end as attribution_flag
      from all_joined_events
      where session_start is not null
      and session_start < ordered_at
      and session_start >= timestamp_sub(ordered_at, INTERVAL 30 DAY)
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    )
    , final_p1 as (
      select *
      , case
      when (sum(attribution_flag) over (partition by user_id) = 0) then "unknown"
      when sum(attribution_flag) over (partition by user_id) > 0 and utm_source is null and referrer_domain not in ("upfaithandfamily.com/", "upfaithandfamily.com", "vhx.tv") then referrer_domain
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
      , event_id
      , session_id
      , device_id
      , advertising_id
      , ip_address
      , user_agent
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
      , referrer_domain
      , referrer_search
      , landing_page
      , source
      , promotion_code
      from final_p1
      where source is not null
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    )
      select *, row_number() over (order by ordered_at) as row from attributable_events
       ;;
    datagroup_trigger: upff_daily_refresh_datagroup
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

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
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

  dimension: landing_page {
    type: string
    sql: ${TABLE}.landing_page ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  set: detail {
    fields: [
      ordered_at_time,
      session_start_time,
      user_id,
      anonymous_id,
      session_id,
      device_id,
      advertising_id,
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
      row,
      promotion_code
    ]
  }
}
