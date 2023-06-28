view: upff_multi_platform_attribution {
  derived_table: {
    sql:
    with attributable_events as (
    -- dedup sessions
      with p0 as (
      select *
      from (
      -- Note: Bigquery produced a weird error when I used the * selector instead of declaring each field.
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
        , source
        from ${upff_web_event_processing.SQL_TABLE_NAME}
        where session_start > timestamp_sub(ordered_at, INTERVAL {% parameter attribution_window %} DAY)
        union all
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
        , source
        from ${upff_ios_event_processing.SQL_TABLE_NAME}
        where session_start > timestamp_sub(ordered_at, INTERVAL {% parameter attribution_window %} DAY)
        union all
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
        , source
        from ${upff_android_event_processing.SQL_TABLE_NAME}
        where session_start > timestamp_sub(ordered_at, INTERVAL {% parameter attribution_window %} DAY)
      )
      where ordered_at between {% date_start date_filter %} and {% date_end date_filter %}
      )
      , p1 as (
        select *
        , row_number() over (partition by user_id, session_start) as event_partition
        from p0
      )
      select * from p1 where event_partition = 1
    )
    , trial_conversion_events as (
    select *
    from ${vimeo_webhook_events.SQL_TABLE_NAME}
    where timestamp between timestamp_add({% date_start date_filter %}, interval 14 day)
    -- we add 21 days to allow for dunning
    and timestamp_add({% date_end date_filter %}, interval 21 day)
    and event in ("customer_product_free_trial_converted")
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
      with meta as (
        SELECT
        "Meta" as ad_platform
        , ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , inline_post_engagements as engagements
        , reach
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from facebook_ads.insights
      )
      , google_ads as (
      SELECT
      "Google Ads" as ad_platform
      , split(ad_id, "::")[OFFSET(1)] as ad_id
      , cast(null as int) as clicks
      , date_start
      , date_stop
      , cast(null as int) as frequency
      , impressions
      , engagements
      , cast(null as int) as reach
      , cast(null as int) as unique_clicks
      , clicks as link_clicks
      , COALESCE((cost/1000000),0 ) as spend
      , cast(null as int) as social_spend
      from adwords.ad_performance_reports
      )
      , google_campaign_manager as (
      SELECT
      "Google Campaign Manager" as ad_platform
      , campaign_id as ad_id
      , cast(null as int) as clicks
      , date_start
      , date_stop
      , cast(null as int) as frequency
      , impressions
      , engagements
      , cast(null as int) as reach
      , cast(null as int) as unique_clicks
      , clicks as link_clicks
      , COALESCE((cost/1000000),0 ) as spend
      , cast(null as int) as social_spend
      from adwords.campaign_performance_reports
      )
      , apple_search as (
        SELECT
        "Apple Search" as ad_platform
        , cast(ad_group_id as string) as ad_id
        , taps as clicks
        , date as date_start
        , date as date_stop
        , cast(null as int) as frequency
        , impressions
        , installs as engagements
        , cast(null as int) as reach
        , cast(null as int) as unique_clicks
        , taps as link_clicks
        , cost as spend
        , cast(null as int) as social_spend
        from customers.apple_search
      )
      , all_platforms_p0 as (
        select
        ad_platform
        , ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , engagements
        , reach
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from meta
        union all
        select
        ad_platform
        , ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , engagements
        , reach
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from google_ads
        union all
        select
        ad_platform
        , ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , engagements
        , reach
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from google_campaign_manager
        union all
        select
        ad_platform
        , ad_id
        , clicks
        , date_start
        , date_stop
        , frequency
        , impressions
        , engagements
        , reach
        , unique_clicks
        , link_clicks
        , spend
        , social_spend
        from apple_search
      )
      , all_platforms_p1 as (
        SELECT *
        , row_number() over (partition by ad_id, date_start order by spend desc) as n
        from all_platforms_p0
      )
      , all_platforms_p2 as (
        select *
        from all_platforms_p1
        where n=1
      )
      select
      ad_platform
      , ad_id
      , clicks
      , date_start
      , date_stop
      , frequency
      , impressions
      , engagements
      , reach
      , unique_clicks
      , link_clicks
      , spend
      , social_spend
      from all_platforms_p2
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
    , replace(a.utm_content, "+", " ") as utm_content
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
    , b.n as touch_point
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
    , g.engagements
    , g.reach
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
    on coalesce(a.ad_id,a.adset_id,a.campaign_id) = g.ad_id and date(a.ordered_at) = date(g.date_start)
    )
    , mofu_final as (
    select * from final
    -- where ordered_at between {% date_start date_filter %}
    -- and {% date_end date_filter %}
    )
    , bofu_final as (
    select
    a.ordered_at
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
    , a.touch_point
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
    , a.engagements
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
    select * from bofu_final
    -- where ordered_at between {% date_start date_filter %}
    -- and timestamp_add({% date_end date_filter %}, interval 14 day)
    )
    select *, row_number() over (order by ordered_at) as row from union_all
    ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  filter: trial_period_filter {
    label: "Period A"
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

  parameter: attribution_model {
    label: "Attribution Model"
    type: unquoted
    default_value: "last_touch"
    allowed_value: {
      label: "Last Touch"
      value: "last_touch"
    }
    allowed_value: {
      label: "First Touch"
      value: "first_touch"
    }
    allowed_value: {
      label: "Equal Credit"
      value: "equal_credit"
    }
    allowed_value: {
      label: "Channel Decay"
      value: "channel_decay"
    }
    allowed_value: {
      label: "Reverse Channel Decay"
      value: "reverse_channel_decay"
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

  dimension: ordered_within_date_range {
    type: yesno
    hidden: yes
    sql: {% condition date_filter %} ${TABLE}.ordered_at {% endcondition %} ;;
  }

  measure: distinct_user_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;

  }

  measure: free_trial_starts {
    type: sum
    sql: ${TABLE}.{% parameter attribution_model %} ;;
    filters: [ordered_within_date_range: "yes", topic: "customer_product_free_trial_created"]
    value_format: "0.##"
  }

  measure: reacquisitions {
    type: sum
    sql: ${TABLE}.{% parameter attribution_model %} ;;
    filters: [ordered_within_date_range: "yes", topic: "customer_product_created"]
    value_format: "0.##"
  }

  measure: free_trial_conversions {
    type: sum
    sql: ${TABLE}.{% parameter attribution_model %} ;;
    filters: [topic: "customer_product_free_trial_converted"]
    value_format: "0.##"
  }

  measure: free_trial_conversion_rate {
    type: number
    sql: 100*${free_trial_conversions}/NULLIF(${free_trial_starts},0) ;;
    value_format: "#\%"
  }

  measure: paying_total {
    type: number
    sql: ${reacquisitions} + ${free_trial_conversions} ;;
    value_format: "0.##"
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

  dimension: composite_ad_id {
    type: string
    sql: CONCAT(${ad_id}, ${ordered_at_date})  ;;
  }

  measure: spend_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.spend ;;
    filters: [ordered_within_date_range: "yes", topic: "customer_product_free_trial_created, customer_product_created"]
    value_format: "$#.00;($#.00)"
  }

  # measure: spend_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ordered_at_date} ;;
  #   sql: ${TABLE}.spend ;;
  #   filters: [ordered_within_date_range: "yes", topic: "customer_product_free_trial_created, customer_product_created"]
  #   value_format: "$#.00;($#.00)"
  # }

  # measure: spend_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.spend ;;
  #   filters: [ordered_within_date_range: "yes"]
  #   value_format: "$#.00;($#.00)"
  # }

  measure: social_spend_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.social_spend ;;
    filters: [ordered_within_date_range: "yes"]
    value_format: "$#.00;($#.00)"
  }

  # measure: social_spend_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.social_spend ;;
  #   filters: [ordered_within_date_range: "yes"]
  #   value_format: "$#.00;($#.00)"
  # }

  measure: clicks_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.clicks ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: clicks_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.clicks ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: engagements_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.engagements ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: engagements_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.engagements ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: unique_clicks_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.unique_clicks ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: unique_clicks_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.unique_clicks ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: link_clicks_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.link_clicks ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: link_clicks_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.link_clicks ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: frequency_average {
    type: average_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.frequency ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: frequency_platform_average {
  #   type: average_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.frequency ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: impressions_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.impressions ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: impressions_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.impressions ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

  measure: reach_total {
    type: sum_distinct
    sql_distinct_key: ${composite_ad_id} ;;
    sql: ${TABLE}.reach ;;
    filters: [ordered_within_date_range: "yes"]
  }

  # measure: reach_platform_total {
  #   type: sum_distinct
  #   sql_distinct_key: ${ad_id} ;;
  #   sql: ${TABLE}.reach ;;
  #   filters: [ordered_within_date_range: "yes"]
  # }

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

  dimension: touch_point {
    type: number
    sql: ${TABLE}.touch_point ;;
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

  dimension: engagements {
    type: number
    sql: ${TABLE}.engagements ;;
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

  dimension: marketing_platform {
    sql: CASE
      WHEN LOWER(${TABLE}.source) LIKE 'hs_email'
        or LOWER(${TABLE}.source) LIKE 'hs_automation'
        then 'HubSpot'
      WHEN LOWER(${TABLE}.source) = 'fb'
        or LOWER(${TABLE}.source) = 'facebook'
        or LOWER(${TABLE}.source) = 'ig'
        or LOWER(${TABLE}.source) = 'an'
        or LOWER(${TABLE}.source) LIKE '%site.source.name%'
        or LOWER(${TABLE}.source) LIKE '%site_source_name%'
        or LOWER(${TABLE}.source) = 'instagram'
        then 'Meta Ads'
      WHEN LOWER(${TABLE}.source) = 'google_ads'
        or LOWER(${TABLE}.source) = 'googleads'
        or LOWER(${TABLE}.source) = 'google adwords'
        then 'Google Ads'
      WHEN LOWER(${TABLE}.source) = 'google marketing platform'
        or LOWER(${TABLE}.source) = 'dv360_upff'
        then 'Google Marketing Platform'
      WHEN LOWER(${TABLE}.source) = 'bing_ads'
        or LOWER(${TABLE}.source) = 'bing_upff'
        or LOWER(${TABLE}.source) = 'bing'
        then 'Bing Ads'
      WHEN LOWER(${TABLE}.source) = 'uptv-linear'
        or LOWER(${TABLE}.source) = 'linear-uptv'
        then 'UPtv Linear'
      WHEN LOWER(${TABLE}.source) = 'uptv_movies_app'
        or LOWER(${TABLE}.source) = 'uptv-web'
        or LOWER(${TABLE}.source) = 'uptv-app'
        or LOWER(${TABLE}.source) = 'uptv'
        or LOWER(${TABLE}.source) = 'uptv.com'
        then 'UPtv Digital'
      WHEN LOWER(${TABLE}.source) = 'zendesk'
        or LOWER(${TABLE}.source) = 'support'
        then 'Customer Support'
      WHEN LOWER(${TABLE}.source) = 'google.com'
        or LOWER(${TABLE}.source) = 'android.gm'
        or LOWER(${TABLE}.source) = 'bing.com'
        or LOWER(${TABLE}.source) = 'yahoo.com'
        or LOWER(${TABLE}.source) = 'duckduckgo.com'
        then 'Organic Search'
      WHEN LOWER(${TABLE}.source) = 'facebook.com'
        or LOWER(${TABLE}.source) = 'instagram.com'
        or LOWER(${TABLE}.source) = 't.co'
        or LOWER(${TABLE}.source) = 'youtube.com'
        then 'Organic Social'
      WHEN LOWER(${TABLE}.source) = 'seedtag'
        then 'Seedtag'
      ELSE 'Others/Unknown'
    END ;;
  }

  dimension: marketing_channel {
    sql:
      case
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%facebook_mobile_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_desktop_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_stories%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_reels%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%instagram_profile_feed%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_stories%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_right_column%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_marketplace%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%facebook_instream_video%'
        OR (LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%' AND ${marketing_platform} = "Meta Ads")
        OR ${marketing_platform} = "Organic Social"
        THEN 'Social Media'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%email%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%eblast%'
        THEN 'Email Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%banner%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%display%'
        OR (LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%' AND ${marketing_platform} = "Google Marketing Platform")
        THEN 'Display Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%paid advertising%'
        OR LOWER(${TABLE}.utm_medium) LIKE '%search%'
        OR LOWER(${TABLE}.utm_medium) = "g"
        THEN 'Search Engine Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%pmax%'
        THEN 'Cross-Platform Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%sms%'
        THEN 'SMS Marketing'
      WHEN LOWER(${TABLE}.utm_medium) LIKE '%ytv%'
        THEN 'Video Marketing'
      when ${marketing_platform} = 'Organic Search'
        then 'Search Engine Optimization'
      -- when ${TABLE}.utm_medium = '' then 'Website'
      -- when ${TABLE}.utm_medium = '' then 'Content Marketing'

      -- when ${TABLE}.utm_medium = '' then 'Affiliate Marketing'
      -- when ${TABLE}.utm_medium = '' then 'Influencer Marketing'
      -- when ${TABLE}.utm_medium = '' then 'TV Advertising'
      -- when ${TABLE}.utm_medium = '' then 'Mobile App'
      ELSE 'Others/Unknown'
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
