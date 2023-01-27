view: upff_android_attribution {
  derived_table: {
    sql:
      with attributable_events as (
        -- dedup sessions
        with p0 as (
        select *
        from (
          select *
          from ${upff_android_event_processing.SQL_TABLE_NAME}
          where session_start > timestamp_sub(ordered_at, INTERVAL {% parameter attribution_window %} DAY)
        )
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
      , trial_conversion_events as (
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
    , conversion_window as (
      select
      user_id
      , ordered_at
      , session_start
      , timestamp_diff(ordered_at, session_start, day) as conversion_window
      from (select * from sources_last_touch where n = 1)
    )
    , paid_media_metrics as (
      with meta_p0 as (
        SELECT
        ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , reach
        , inline_post_engagements
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from `up-faith-and-family-216419.facebook_ads.insights`
        where date_start between timestamp_sub({% date_start date_filter %}, interval 15 day)
        and {% date_end date_filter %}
      )
      , meta_p1 as (
        SELECT *
        , row_number() over (partition by ad_id, date_start order by spend desc) as n
        from meta_p0
      )
      , meta_p2 as (
        select *
        from meta_p1
        where n=1
      )
      select
      ad_id
      , clicks
      , date_start
      , date_stop
      , frequency
      , impressions
      , reach
      , inline_post_engagements
      , unique_clicks
      , link_clicks
      , spend
      , social_spend
      from meta_p2
    )
    , final as (
    select
    a.ordered_at
    , a.session_start
    , f.conversion_window
    , a.user_id
    , a.anonymous_id
    , a.device_id
    , a.ip_address
    , a.plan_type
    , a.platform
    , a.topic
    , a.utm_content
    , a.utm_medium
    -- hotfixing a bug where the plus sign is coming though instead of a space
    , replace(a.utm_campaign, "+", " ") as utm_campaign
    , a.utm_source
    , a.utm_term
    , a.ad_id
    , a.adset_id
    , a.campaign_id
    , a.user_agent
    , a.referrer_domain
    , a.referrer_search
    , a.source
    , b.credit as last_touch
    , c.credit as first_touch
    , d.credit as equal_credit
    , e.channel_decay
    , e.reverse_channel_decay
    , g.clicks
    , g.date_start
    , g.date_stop
    , g.frequency
    , g.impressions
    , g.reach
    , g.inline_post_engagements
    , g.unique_clicks
    , g.link_clicks
    , g.spend
    , g.social_spend
    from attributable_events a
    inner join last_touch_v2 b
    on a.user_id = b.user_id and a.session_start = b.session_start
    inner join first_touch_v2 c
    on a.user_id = c.user_id and a.session_start = c.session_start
    inner join linear_v2 d
    on a.user_id = d.user_id and a.session_start = d.session_start
    inner join channel_decay e
    on a.user_id = e.user_id and a.session_start = e.session_start
    inner join conversion_window f
    on a.user_id = f.user_id and a.ordered_at = f.ordered_at
    left join paid_media_metrics g
    on a.ad_id = g.ad_id and date(a.ordered_at) = date(g.date_start)
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
    , a.conversion_window
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
    , a.ad_id
    , a.adset_id
    , a.campaign_id
    , a.user_agent
    , a.referrer_domain
    , a.referrer_search
    , a.source
    , a.last_touch
    , a.first_touch
    , a.equal_credit
    , a.channel_decay
    , a.reverse_channel_decay
    , a.clicks
    , a.date_start
    , a.date_stop
    , a.frequency
    , a.impressions
    , a.reach
    , a.inline_post_engagements
    , a.unique_clicks
    , a.link_clicks
    , a.spend
    , a.social_spend
    from final a
    inner join trial_conversion_events as b
    on a.user_id = b.user_id
    )
    , union_all as (
    select * from mofu_final
    union all
    select * from tofu_final
    where ordered_at between {% date_start date_filter %} and {% date_end date_filter %}
    )
    select *, row_number() over (order by ordered_at) as row from union_all
    ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  parameter: attribution_window {
    label: "Attribution Window"
    type: number
    default_value: "30"
    allowed_value: {
      label: "1 day"
      value: "1"
    }
    allowed_value: {
      label: "3 days"
      value: "3"
    }
    allowed_value: {
      label: "7 days"
      value: "7"
    }
    allowed_value: {
      label: "14 days"
      value: "14"
    }
    allowed_value: {
      label: "30 days"
      value: "30"
    }
  }

  dimension: row {
    primary_key: yes
    type: number
    sql: ${TABLE}.row ;;
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
    value_format: "0.##"
  }

  measure: channel_decay_total {
    type: sum
    sql: ${TABLE}.channel_decay ;;
    value_format: "0.##"
  }

  measure: reverse_channel_decay_total {
    type: sum
    sql: ${TABLE}.reverse_channel_decay ;;
    value_format: "0.##"
  }

  measure: spend_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.spend ;;
    value_format: "$#.00;($#.00)"
  }

  measure: social_spend_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.social_spend ;;
    value_format: "$#.00;($#.00)"
  }

  measure: clicks_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.clicks ;;
  }

  measure: inline_post_engagements_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.inline_post_engagements ;;
  }

  measure: unique_clicks_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.unique_clicks ;;
  }

  measure: link_clicks_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.link_clicks ;;
  }

  measure: frequency_average {
    type: average_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.frequency ;;
  }

  measure: impressions_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.impressions ;;
  }

  measure: reach_total {
    type: sum_distinct
    sql_distinct_key: ${ad_id} ;;
    sql: ${TABLE}.reach ;;
  }

  dimension_group: ordered_at {
    type: time
    sql: ${TABLE}.ordered_at ;;
  }

  dimension_group: session_start {
    type: time
    sql: ${TABLE}.session_start ;;
  }

  dimension: conversion_window {
    type: number
    sql: ${TABLE}.conversion_window ;;
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

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: referrer_search {
    type: string
    sql: ${TABLE}.referrer_search ;;
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

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: frequency {
    type: number
    sql: ${TABLE}.frequency ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: reach {
    type: number
    sql: ${TABLE}.reach ;;
  }

  dimension: inline_post_engagements {
    type: number
    sql: ${TABLE}.inline_post_engagements ;;
  }

  dimension: unique_clicks {
    type: number
    sql: ${TABLE}.unique_clicks ;;
  }

  dimension: link_clicks {
    type: number
    sql: ${TABLE}.link_clicks ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  dimension: social_spend {
    type: number
    sql: ${TABLE}.social_spend ;;
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
