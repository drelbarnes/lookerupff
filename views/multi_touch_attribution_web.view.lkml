view: multi_touch_attribution_web {
  derived_table: {
    sql:
      -- JOIN ORDERS ON PAGE VISITS
      with web_orders as (
        with order_completed_events as (
          select timestamp as ordered_at
          , user_id as user_id
          , anonymous_id
          , context_ip as ip_address
          , context_traits_cross_domain_id as cross_domain_id
          , platform
          from javascript.order_completed
          where
          timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        )
        , purchase_events as (
        select
        user_id
        , event as topic
        , subscription_frequency as plan_type
        from ${vimeo_webhook_events.SQL_TABLE_NAME}
        where event in ("customer_product_created", "customer_product_free_trial_created")
        and
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
        )
        select
        a.ordered_at
        , a.user_id
        , a.anonymous_id
        , a.ip_address
        , a.cross_domain_id
        , a.platform
        , b.plan_type
        , b.topic
        from order_completed_events as a
        left join purchase_events as b
        on a.user_id = b.user_id
      )
      , web_pages as (
       with app_pages as (
          select
          timestamp as viewed_at
        , anonymous_id
        , context_ip as ip_address
        , context_traits_cross_domain_id as cross_domain_id
        , context_campaign_content as utm_content
        , context_campaign_medium as utm_medium
        , context_campaign_name as utm_campaign
        , context_campaign_source as utm_source
        , context_campaign_term as utm_term
        , context_page_referrer as referrer
        , view
        , context_user_agent as user_agent
        from javascript.pages
        where
        timestamp >= TIMESTAMP_ADD({% date_start date_filter %}, INTERVAL -({% parameter attribution_window %} - 1) DAY)
        and timestamp <= {% date_end date_filter %}
        )
        , site_pages as (
          select
          timestamp as viewed_at
        , anonymous_id
        , context_ip as ip_address
        , context_traits_cross_domain_id as cross_domain_id
        , context_campaign_content as utm_content
        , context_campaign_medium as utm_medium
        , context_campaign_name as utm_campaign
        , context_campaign_source as utm_source
        , context_campaign_term as utm_term
        , context_page_referrer as referrer
        , title as view
        , context_user_agent as user_agent
        from javascript_upff_home.pages
        where
        timestamp >= TIMESTAMP_ADD({% date_start date_filter %}, INTERVAL -({% parameter attribution_window %} - 1) DAY)
        and timestamp <= {% date_end date_filter %}
        )
        , pages_union as (
          select * from app_pages
          union all
          select * from site_pages
        )
        select *
        from pages_union
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
        on web_pages.anonymous_id = web_orders.anonymous_id
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
      )
      , web_pages_web_orders_cross_domain as (
        select
        web_orders.ordered_at
        , web_orders.user_id
        , web_orders.plan_type
        , web_orders.platform
        , web_orders.topic
        , web_pages.*
        from web_orders
        full join web_pages
        on web_pages.cross_domain_id = web_orders.cross_domain_id
      )
      , all_orders as (
        select * from web_pages_web_orders_anon
        union all
        select * from web_pages_web_orders_ip
        union all
        select * from web_pages_web_orders_cross_domain
      )
      -- ATTRIBUITION MODELS
      -- Multitouch source attribution, channel decay
      --  create paid column with value = 0 if utm param null else 1
      , attributable_orders as (
      select
      ordered_at
      , viewed_at
      , user_id
      , anonymous_id
      , ip_address
      , cross_domain_id
      , plan_type
      , platform
      , topic
      , utm_content
      , utm_medium
      , utm_campaign
      , utm_source
      , utm_term
      , view
      , referrer
      , user_agent
      , case
      when utm_source is null then 0
      else 1
      end as paid
      from all_orders
      where viewed_at is not null
      and viewed_at < ordered_at
      and viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(ordered_at, DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)

      )
      --  by user_id, case when sum(paid) = 0, then source = organic
      --   else strip out null records and group utm sources
      , sources as (
      select *
      , case
      when sum(paid) over (partition by user_id) = 0 then "organic"
      else utm_source
      end as source
      from attributable_orders
      )
      , ranked as (
      with grouped as (
      select distinct
      ordered_at
      , viewed_at
      , user_id
      , anonymous_id
      , ip_address
      , cross_domain_id
      , plan_type
      , platform
      , topic
      , utm_content
      , utm_medium
      , utm_campaign
      , utm_source
      , utm_term
      , view
      , referrer
      , user_agent
      , source
      from sources
      where source is not null
      )
      select *
      , row_number() over (partition by user_id order by viewed_at {% parameter attribution_method %}) as n
      from grouped
      )
      , conversion_attribution as (
      select
      ordered_at
      , datetime_trunc(viewed_at, hour) as viewed_at
      , user_id
      , anonymous_id
      , ip_address
      , cross_domain_id
      , plan_type
      , platform
      , topic
      , utm_content
      , utm_medium
      , utm_campaign
      , utm_source
      , utm_term
      , view
      , referrer
      , user_agent
      , source
      , case when n = max(n) over (partition by user_id) then 1 else 0
      end as conversion_event
      , n
      from ranked
      )
      , single_touch as (
      select
      user_id
      , viewed_at
      , source
      , conversion_event as score
      , n
      from conversion_attribution
      )
      , linear as (
      select
      user_id
      , viewed_at
      , source
      , safe_cast(round(1/max(n) over (partition by user_id), 4) as string) as score
      , n
      FROM conversion_attribution
      )
      , channel_decay as (
      with weighted as (
      SELECT
      user_id
      , viewed_at
      , source
      , safe_cast(round(pow(2,-n/(max(n) over (partition by user_id)/2)), 4) as float64) as weights
      , n
      FROM conversion_attribution
      )
      SELECT
      user_id
      , viewed_at
      , source
      , ROUND(
      IF(
      SAFE_CAST(weights AS FLOAT64)=0 OR SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY user_id)=0
      , 0
      , SAFE_CAST(weights AS FLOAT64)/SUM(SAFE_CAST(weights AS FLOAT64)) OVER (PARTITION BY user_id)
      )
      , 2) AS score
      , n
      FROM weighted
      )
      , final as (
      select
      a.ordered_at
      , a.viewed_at
      , a.user_id
      , a.anonymous_id
      , a.ip_address
      , a.cross_domain_id
      , a.plan_type
      , a.platform
      , a.topic
      , a.utm_content
      , a.utm_medium
      , a.utm_campaign
      , a.utm_source
      , a.utm_term
      , a.user_agent
      , a.view
      , a.referrer
      , a.source
      , a.conversion_event
      , safe_cast(b.score as float64) as credit
      from conversion_attribution as a
      inner join {% parameter attribution_model %} as b
      on a.user_id = b.user_id and a.n = b.n
      )
      , non_attributable_orders as (
        select
        ordered_at
        , viewed_at
        , user_id
        , anonymous_id
        , ip_address
        , cross_domain_id
        , plan_type
        , platform
        , topic
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , user_agent
        , view
        , referrer
        , cast(null as string) as source
        , 1 as conversion_event
        , null as credit
        from all_orders
        where viewed_at is null
        and user_id not in (select user_id from final)
        and ordered_at >= {% date_start date_filter %} and ordered_at <= {% date_end date_filter %}
      )
      , unconverted_page_views as (
        select
        cast(null as timestamp) as ordered_at
        , viewed_at
        , cast(null as string) as user_id
        , anonymous_id
        , ip_address
        , cross_domain_id
        , cast(null as string) as plan_type
        , "web" as platform
        , cast(null as string) as topic
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , user_agent
        , view
        , referrer
        , cast(null as string) as source
        , null as conversion_event
        , null as credit
        from web_pages
        where anonymous_id not in (
          select distinct anonymous_id from final
        )
        and
        viewed_at >= {% date_start date_filter %} and viewed_at <= {% date_end date_filter %}
      )
      , union_all as (
      select * from final
      union all
      select * from non_attributable_orders
      union all
      select * from unconverted_page_views
      )
      select *, row_number() over (order by ordered_at) as row from union_all
      ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  parameter: attribution_method {
    type: unquoted
    label: "Attribution Method"
    allowed_value: {
      label: "First Event"
      value: "desc"
    }
    allowed_value: {
      label: "Last Event"
      value: "asc"
    }
  }

  parameter: attribution_model {
    type: unquoted
    label: "Attribution Model"
    allowed_value: {
      label: "Single Touch"
      value: "single_touch"
    }
    allowed_value: {
      label: "Equal Credit"
      value: "linear"
    }
    allowed_value: {
      label: "Channel Decay"
      value: "channel_decay"
    }
  }

  parameter: attribution_window {
    type: unquoted
    label: "Attribution Window"
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

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
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

  dimension_group: ordered_at {
    type: time
    sql: ${TABLE}.ordered_at ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: plan_type {
    type: string
    sql: ${TABLE}.plan_type ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension_group: viewed_at {
    type: time
    sql: ${TABLE}.viewed_at ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: campaign_source {
    sql: CASE
              WHEN ${TABLE}.source IS NULL then 'Direct Traffic'
              WHEN ${TABLE}.source = 'organic' then 'Direct Traffic'
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

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: conversion_event {
    type: number
    sql: ${TABLE}.conversion_event ;;
  }

  dimension: credit {
    type: number
    sql: ${TABLE}.credit ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
    sql: ${user_id};;
  }

  measure: distinct_count_attributed {
    type:  count_distinct
    sql:
      CASE
        WHEN (${TABLE}.credit  > 0) AND NOT (${TABLE}.credit  IS NULL) THEN ${TABLE}.user_id
        ELSE NULL
      END ;;
  }

  measure: distinct_facebook_count {
    type: count_distinct
    sql:CASE
          WHEN ${TABLE}.utm_source = 'fb' THEN ${user_id}
          WHEN ${TABLE}.utm_source = 'ig' THEN ${user_id}
          WHEN ${TABLE}.utm_source LIKE '%site.source.name%' then ${user_id}
          WHEN ${TABLE}.utm_source LIKE '%site_source_name%' then ${user_id}
    END ;;
  }

  measure: distinct_google_count {
    type: count_distinct
    sql: ${user_id};;
    filters: [utm_source: "google_ads"]
  }

  measure: distinct_bing_count {
    type: count_distinct
    sql: ${user_id};;
    filters: [utm_source: "bing_ads"]
  }

  measure: distinct_organic_count {
    type: count_distinct
    sql:CASE
          WHEN ${TABLE}.utm_source IS NULL THEN ${user_id}
       END ;;
  }

  measure: total_credit {
    type: sum
    sql: ${credit} ;;
    value_format: "0.##"
  }

  set: detail {
    fields: [
      ordered_at_time
      , viewed_at_time
      , user_id
      , anonymous_id
      , ip_address
      , cross_domain_id
      , plan_type
      , platform
      , utm_content
      , utm_medium
      , utm_campaign
      , utm_source
      , utm_term
      , user_agent
      , view
      , referrer
      , source
      , credit
    ]
  }
}
