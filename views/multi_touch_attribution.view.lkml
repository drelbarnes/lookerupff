view: multi_touch_attribution {
  derived_table: {
    sql:
      -- JOIN ORDERS ON PAGE VISITS
      with orders as (
        select orders.timestamp as ordered_at
        , orders.user_id as user_id
        , orders.anonymous_id
        , orders.context_ip
        , orders.context_traits_cross_domain_id
        , orders.context_revenue as revenue
        , orders.platform
        from `up-faith-and-family-216419.javascript.order_completed` as orders
        where
        orders.timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY)
        and
        orders.timestamp < TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter order_window %} - 1) DAY), INTERVAL {% parameter order_window %} DAY)
      )
      , app_pages as (
        select
        timestamp as viewed_at
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , context_campaign_content as utm_content
      , context_campaign_medium as utm_medium
      , context_campaign_name as utm_campaign
      , context_campaign_source as utm_source
      , context_campaign_term as utm_term
      , context_page_referrer as referrer
      , view
      , context_user_agent as user_agent
      from `up-faith-and-family-216419.javascript.pages`
      )
      , web_pages as (
        select
        timestamp as viewed_at
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , context_campaign_content as utm_content
      , context_campaign_medium as utm_medium
      , context_campaign_name as utm_campaign
      , context_campaign_source as utm_source
      , context_campaign_term as utm_term
      , context_page_referrer as referrer
      , title as view
      , context_user_agent as user_agent
      from `up-faith-and-family-216419.javascript_upff_home.pages`
      )
      , app_orders_anon as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , app_pages.*
        from orders
        left join app_pages
        on app_pages.anonymous_id = orders.anonymous_id
        where
        app_pages.viewed_at < orders.ordered_at
        and
        app_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , web_orders_anon as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , web_pages.*
        from orders
        left join web_pages
        on web_pages.anonymous_id = orders.anonymous_id
        where
        web_pages.viewed_at < orders.ordered_at
        and
        web_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , app_orders_ip as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , app_pages.*
        from orders
        left join app_pages
        on app_pages.context_ip = orders.context_ip
        where
        app_pages.viewed_at < orders.ordered_at
        and
        app_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , web_orders_ip as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , web_pages.*
        from orders
        left join web_pages
        on web_pages.context_ip = orders.context_ip
        where
        web_pages.viewed_at < orders.ordered_at
        and
        web_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , app_orders_cross_domain as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , app_pages.*
        from orders
        left join app_pages
        on app_pages.context_traits_cross_domain_id = orders.context_traits_cross_domain_id
        where
        app_pages.viewed_at < orders.ordered_at
        and
        app_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , web_orders_cross_domain as (
        select
        orders.ordered_at
        , orders.user_id
        , orders.revenue
        , orders.platform
        , web_pages.*
        from orders
        left join web_pages
        on web_pages.context_traits_cross_domain_id = orders.context_traits_cross_domain_id
        where
        web_pages.viewed_at < orders.ordered_at
        and
        web_pages.viewed_at >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY)
      )
      , all_orders as (
        select * from app_orders_anon
        union all
        select * from web_orders_anon
        union all
        select * from app_orders_ip
        union all
        select * from web_orders_ip
        union all
        select * from app_orders_cross_domain
        union all
        select * from web_orders_cross_domain
      )

      -- ATTRIBUITION MODELS
      -- Multitouch source attribution, channel decay
      --  create paid column with value = 0 if utm param null else 1

      , flag_paid as (
      select
      ordered_at
      , datetime_trunc(viewed_at, hour) as viewed_at
      , user_id
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , revenue
      , platform
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
      )
      --  by user_id, case when sum(paid) = 0, then source = organic
      --   else strip out null records and group utm sources
      , sources as (
      select *
      , case
      when sum(paid) over (partition by user_id) = 0 then "organic"
      else utm_source
      end as source
      from flag_paid
      )
      , ranked as (
      with grouped as (
      select
      ordered_at
      , viewed_at
      , user_id
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , revenue
      , platform
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
      group by 1,2,3,4,5,6,7,8,9,10,11,12
      )
      select *
      , row_number() over (partition by user_id order by viewed_at {% parameter attribution_method %}) as n
      from grouped
      )
      , conversion_attribution as (
      select
      ordered_at
      , viewed_at
      , user_id
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , revenue
      , platform
      , utm_content
      , utm_medium
      , utm_campaign
      , utm_source
      , utm_term
      , view
      , referrer
      , user_agent
      , source
      , case when n = min(n) over (partition by user_id) then 1 else 0
      end as event
      , n
      from ranked
      )
      , single_touch as (
      select
      user_id
      , viewed_at
      , source
      , event as score
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
      , CASE
      WHEN viewed_at = FIRST_VALUE(viewed_at) OVER (PARTITION BY user_id ORDER BY viewed_at) AND MAX(event) OVER (PARTITION BY user_id) = 1
      THEN SAFE_CAST(1.1-ROW_NUMBER() OVER (PARTITION BY user_id) AS STRING)
      WHEN viewed_at > LAG(viewed_at) OVER (PARTITION BY user_id ORDER BY viewed_at) AND MAX(event) OVER (PARTITION BY user_id) = 1
      THEN SAFE_CAST(ROUND(1.1-1/ROW_NUMBER() OVER (PARTITION BY user_id), 4) AS STRING)
      ELSE 'null'
      END AS weights
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
      select
      a.ordered_at
      , a.viewed_at
      , a.user_id
      , a.anonymous_id
      , a.context_ip
      , a.context_traits_cross_domain_id
      , a.revenue
      , a.platform
      , a.utm_content
      , a.utm_medium
      , a.utm_campaign
      , a.utm_source
      , a.utm_term
      , a.user_agent
      , a.view
      , a.referrer
      , a.source
      , b.score as credit
      from conversion_attribution as a
      inner join {% parameter attribution_model %} as b
      on a.user_id = b.user_id and a.n = b.n
      ;;
  }

  parameter: attribution_method {
    type: unquoted
    label: "Attribution Method"
    allowed_value: {
      label: "First Interaction"
      value: "asc"
    }
    allowed_value: {
      label: "Last Interaction"
      value: "desc"
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
      label: "Linear"
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
    label: "Order Window"
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

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: context_traits_cross_domain_id {
    type: string
    sql: ${TABLE}.context_traits_cross_domain_id ;;
  }

  dimension_group: ordered_at {
    type: time
    sql: ${TABLE}.ordered_at ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: revenue {
    type: string
    sql: ${TABLE}.revenue ;;
  }

  dimension: plan_type {
    sql: CASE
              WHEN ${TABLE}.revenue = 53.99 then 'yearly'
              WHEN ${TABLE}.revenue = 5.99 then 'monthly'
          END ;;
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
              WHEN ${TABLE}.source IS NULL then 'Organic'
              WHEN ${TABLE}.source LIKE '%site.source.name%' then 'Facebook Ads'
              WHEN ${TABLE}.source LIKE '%site_source_name%' then 'Facebook Ads'
              WHEN ${TABLE}.source = 'google_ads' then 'Google Ads'
              WHEN ${TABLE}.source = 'GoogleAds' then 'Google Ads'
              WHEN ${TABLE}.source = 'fb' then 'Facebook Ads'
              WHEN ${TABLE}.source = 'ig' then 'Facebook Ads'
              WHEN ${TABLE}.source = 'bing_ads' then 'Bing Ads'
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

  set: detail {
    fields: [
      ordered_at_time
      , viewed_at_time
      , user_id
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , revenue
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
