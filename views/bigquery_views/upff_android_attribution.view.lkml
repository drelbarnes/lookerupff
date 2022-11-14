view: upff_android_attribution {
  derived_table: {
    sql: with attributable_events as (
      -- dedup sessions
      with p0 as (
      select *
      from ${upff_android_event_processing.SQL_TABLE_NAME}
      where ordered_at between timestamp_sub({% date_start date_filter %}, interval 15 day)
      and {% date_end date_filter %}
      )
      , p1 as (
      select *
      , row_number() over (partition by device_id, session_start) as event_partition
      from p0
      )
      select * from p1 where event_partition = 1
      )
      , conversion_events as (
      select *
      from ${vimeo_webhook_events.SQL_TABLE_NAME}
      where timestamp between {% date_start date_filter %}
      and {% date_end date_filter %}
      and event in ("customer_product_free_trial_converted")
      and platform = "android"
      )
      , sources_last_touch as (
      select *
      , row_number() over (partition by user_id order by session_start asc) as n
      from attributable_events
      )
      , sources_first_touch as (
      select *
      , row_number() over (partition by user_id order by session_start desc) as n
      from attributable_events
      )
      , last_touch_v2 as (
      with p2 as (
      select
      user_id
      , session_start
      , source
      , case when n = max(n) over (partition by user_id) then 1 else 0
      end as conversion_event
      , n
      from sources_last_touch
      )
      select
      user_id
      , session_start
      , source
      , conversion_event as credit
      , n
      from p2
      )
      , first_touch_v2 as (
      with p2 as (
      select
      user_id
      , session_start
      , source
      , case when n = max(n) over (partition by user_id) then 1 else 0
      end as conversion_event
      , n
      from sources_first_touch
      )
      select
      user_id
      , session_start
      , source
      , conversion_event as credit
      , n
      from p2
      )
      , linear_v2 as (
      select
      user_id
      , session_start
      , source
      , safe_cast(round(1/max(n) over (partition by user_id), 4) as float64) as credit
      , n
      FROM sources_last_touch
      )
      , channel_decay as (
      with p0 as (
      select
      user_id
      , session_start
      , source
      , safe_cast(round(pow(2,-n/(max(n) over (partition by user_id)/2)), 4) as float64) as weights
      , n
      from sources_first_touch
      )
      , p1 as (
      select
      user_id
      , session_start
      , source
      , safe_cast(round(pow(2,-n/(max(n) over (partition by user_id)/2)), 4) as float64) as reverse_weights
      , n
      from sources_last_touch
      )
      select
      p0.user_id
      , p0.session_start
      , p0.source
      , round(
      if(
      safe_cast(p0.weights AS FLOAT64)=0 or sum(safe_cast(p0.weights as FLOAT64)) over (partition by p0.user_id)=0
      , 0
      , safe_cast(p0.weights AS FLOAT64)/sum(safe_cast(p0.weights AS FLOAT64)) over (partition by p0.user_id)
      )
      , 2) as channel_decay
      , round(
      if(
      safe_cast(p1.reverse_weights AS FLOAT64)=0 or sum(safe_cast(p1.reverse_weights as FLOAT64)) over (partition by p1.user_id)=0
      , 0
      , safe_cast(p1.reverse_weights AS FLOAT64)/sum(safe_cast(p1.reverse_weights AS FLOAT64)) over (partition by p1.user_id)
      )
      , 2) as reverse_channel_decay
      from p0
      left join p1
      on p0.user_id = p1.user_id and p0.session_start = p1.session_start
      )
      , final as (
      select
      a.ordered_at
      , a.session_start
      , a.user_id
      , a.anonymous_id
      , a.device_id
      , a.ip_address
      , a.plan_type
      , a.platform
      , a.topic
      , a.utm_content
      , a.utm_medium
      , a.utm_campaign
      , a.utm_source
      , a.utm_term
      , a.user_agent
      , a.referrer_domain
      , a.source
      , b.credit as last_touch
      , c.credit as first_touch
      , d.credit as equal_credit
      , e.channel_decay
      , e.reverse_channel_decay
      from attributable_events a
      inner join last_touch_v2 b
      on a.user_id = b.user_id and a.session_start = b.session_start
      inner join first_touch_v2 c
      on a.user_id = c.user_id and a.session_start = c.session_start
      inner join linear_v2 d
      on a.user_id = d.user_id and a.session_start = d.session_start
      inner join channel_decay e
      on a.user_id = e.user_id and a.session_start = e.session_start
      )
      , mofu_final as (
      select * from final
      where ordered_at between {% date_start date_filter %}
      and {% date_end date_filter %}
      )
      , tofu_final as (
      select
      b.timestamp as ordered_at
      , a.session_start
      , a.user_id
      , a.anonymous_id
      , a.device_id
      , a.ip_address
      , a.plan_type
      , a.platform
      , b.event as topic
      , a.utm_content
      , a.utm_medium
      , a.utm_campaign
      , a.utm_source
      , a.utm_term
      , a.user_agent
      , a.referrer_domain
      , a.source
      , a.last_touch
      , a.first_touch
      , a.equal_credit
      , a.channel_decay
      , a.reverse_channel_decay
      from final a
      inner join conversion_events as b
      on a.user_id = b.user_id
      )
      , union_all as (
      select * from mofu_final
      union all
      select * from tofu_final
      where ordered_at between {% date_start date_filter %} and {% date_end date_filter %}

      )
      select * from union_all order by user_id, ordered_at, session_start
      ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }
  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: last_touch_total {
    type: sum
    sql: ${TABLE}.last_touch ;;
  }

  measure: first_touch_total {
    type: sum
    sql: ${TABLE}.first_touch ;;
  }

  measure: equal_credit_total {
    type: sum
    sql: ${TABLE}.equal_credit ;;
  }

  measure: channel_decay_total {
    type: sum
    sql: ${TABLE}.channel_decay ;;
  }

  measure: reverse_channel_decay_total {
    type: sum
    sql: ${TABLE}.reverse_channel_decay ;;
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

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: last_touch {
    type: number
    sql: ${TABLE}.last_touch ;;
  }

  dimension: first_touch {
    type: number
    sql: ${TABLE}.first_touch ;;
  }

  dimension: equal_credit {
    type: number
    sql: ${TABLE}.equal_credit ;;
  }

  dimension: channel_decay {
    type: number
    sql: ${TABLE}.channel_decay ;;
  }

  dimension: reverse_channel_decay {
    type: number
    sql: ${TABLE}.reverse_channel_decay ;;
  }

  dimension: campaign_source {
    sql: CASE
      WHEN ${TABLE}.source IS NULL then 'Unknown'
      WHEN ${TABLE}.source = 'organic' then 'Unknown'
      WHEN ${TABLE}.source LIKE 'hs_email' then 'Internal'
      WHEN ${TABLE}.source LIKE 'hs_automation' then 'Internal'
      WHEN ${TABLE}.source LIKE '%site.source.name%' then 'Facebook Ads'
      WHEN ${TABLE}.source LIKE '%site_source_name%' then 'Facebook Ads'
      WHEN ${TABLE}.source = 'google_ads' then 'Google Ads'
      WHEN ${TABLE}.source = 'GoogleAds' then 'Google Ads'
      WHEN ${TABLE}.source = 'fb' then 'Facebook Ads'
      WHEN ${TABLE}.source = 'facebook' then 'Facebook Ads'
      WHEN ${TABLE}.source = 'ig' then 'Facebook Ads'
      WHEN ${TABLE}.source = 'bing_ads' then 'Bing Ads'
      WHEN ${TABLE}.source = 'an' then 'Facebook Ads'
      else ${TABLE}.source
    END ;;
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
      user_agent,
      referrer_domain,
      source,
      last_touch,
      first_touch,
      equal_credit,
      channel_decay,
      reverse_channel_decay
    ]
  }
}
