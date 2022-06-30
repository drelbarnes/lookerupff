view: upff_web_pages {
  derived_table: {
    sql: with site_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "page_view" as event
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , case
            when context_page_path = "/stream/" and context_campaign_source is null
              then "direct"
            when context_page_path = "/getupff/" and context_campaign_source is null
              then "direct"
            else safe_cast(context_campaign_source as string)
            end as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(title as string) as view
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , "" as session_id
        , "web" as platform
        , timestamp
        from javascript_upff_home.pages
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
      )
      , app_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "page_view" as event
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(title as string) as view
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , safe_cast(session_id as string) as session_id
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.pages
        where
        timestamp >= {% date_start date_filter %} and timestamp <= {% date_end date_filter %}
      )
      , union_all as (
        select * FROM site_pages
        union all
        select * from app_pages
      )
      select *, row_number() over (order by timestamp) as row from union_all

       ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
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

  parameter: time_period {
    type: unquoted
    label: "Period"
    allowed_value: {
      label: "Last 7 days"
      value: "7"
    }
    allowed_value: {
      label: "Last 14 days"
      value: "14"
    }
    allowed_value: {
      label: "Last 28 days"
      value: "28"
    }
    allowed_value: {
      label: "Last 30 days"
      value: "30"
    }
    allowed_value: {
      label: "Last 60 days"
      value: "60"
    }
    allowed_value: {
      label: "Last 90 days"
      value: "90"
    }

    allowed_value: {
      label: "Last 180 days"
      value: "180"
    }

    allowed_value: {
      label: "Last 365 days"
      value: "365"
    }
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count_anonymous_id {
    type: count_distinct
    sql:${anonymous_id} ;;
    drill_fields: [detail*]
  }

  dimension: row {
    primary_key: yes
    type: number
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

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
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

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: view {
    type: string
    sql: ${TABLE}.view ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: path {
    type: string
    sql: ${TABLE}.path ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: campaign_source {
    sql: CASE
              WHEN ${TABLE}.utm_source IS NULL then 'Organic'
              WHEN ${TABLE}.utm_source = 'organic' then 'Organic'
              WHEN ${TABLE}.utm_source LIKE 'hs_email' then 'Internal'
              WHEN ${TABLE}.utm_source LIKE 'hs_automation' then 'Internal'
              WHEN ${TABLE}.utm_source LIKE '%site.source.name%' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source LIKE '%site_source_name%' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'google_ads' then 'Google Ads'
              WHEN ${TABLE}.utm_source = 'GoogleAds' then 'Google Ads'
              WHEN ${TABLE}.utm_source = 'fb' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'facebook' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'ig' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'bing_ads' then 'Bing Ads'
              WHEN ${TABLE}.utm_source = 'an' then 'Facebook Ads'
              else ${TABLE}.utm_source
            END ;;
  }

  set: detail {
    fields: [
      user_id,
      anonymous_id,
      ip_address,
      cross_domain_id,
      event,
      utm_content,
      utm_medium,
      utm_campaign,
      utm_source,
      utm_term,
      referrer,
      view,
      url,
      path,
      device,
      session_id,
      timestamp_time
    ]
  }
}
