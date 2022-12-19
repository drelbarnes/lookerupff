view: upff_web_sessions {
  derived_table: {
    sql: with page_events as (
        select
        cast(timestamp as timestamp) as timestamp
        , event_number
        , anonymous_id
        , ip_address
        , user_agent
        , cross_domain_id
        , user_id
        , event
        , event_id
        , session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , utm_campaign
        , utm_source
        , utm_medium
        , utm_content
        , utm_term
        , referrer
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , search
        , title
        , url
        , path
        , platform
        from ${upff_page_events.SQL_TABLE_NAME}
      )
      , sessions_p0 as (
        with first_values as (
          select
          session_id
          , first_value(anonymous_id) over (partition by session_id order by event_number) as anonymous_id
          , first_value(ip_address) over (partition by session_id order by event_number) as ip_address
          , first_value(user_agent) over (partition by session_id order by event_number) as user_agent
          , first_value(event_id) over (partition by session_id order by event_number) as event_id
          , first_value(timestamp) over (partition by session_id order by event_number) as session_start
          , first_value(timestamp) over (partition by session_id order by event_number desc) as session_end
          , first_value(path) over (partition by session_id order by event_number) as landing_page
          , first_value(path) over (partition by session_id order by event_number desc) as exit_page
          , first_value(is_conversion ignore nulls) over (partition by session_id order by is_conversion desc) as conversion
          , case
            when is_session_start =1 and is_session_end =1 then 1
            else 0
            end as bounce
          from page_events
        )
        select * from first_values group by 1,2,3,4,5,6,7,8,9,10,11
      )
      , sessions_p1 as (
        with sessions_utm_values as (
          select
          session_id
          , string_agg(utm_campaign) over (partition by session_id) as utm_campaign_values
          , string_agg(utm_source) over (partition by session_id) as utm_source_values
          , string_agg(utm_medium) over (partition by session_id) as utm_medium_values
          , string_agg(utm_content) over (partition by session_id) as utm_content_values
          , string_agg(utm_term) over (partition by session_id) as utm_term_values
          from page_events
          group by session_id, utm_campaign, utm_source, utm_medium, utm_content, utm_term
        )
        select * from sessions_utm_values group by 1,2,3,4,5,6
      )
      , paths as (
        with p0 as (
          select
          session_id
          , event_number
          , path
          , event
          , first_value(is_conversion ignore nulls) over (partition by session_id order by is_conversion desc) as conversion
          from page_events
          group by session_id, event_number, path, event, is_conversion
        )
        select *
        , lag(path) over (partition by session_id order by event_number) as path_lag
        from p0
      )
      , sessions_p2 as (
        select
        session_id
        , count(distinct path) over (partition by session_id) as unique_pages_viewed
        from paths
        group by session_id, path
      )
      , max_events as (select max(event_number) over (partition by session_id) from page_events)
      -- any sessions with more than 50 touchpoints are in the 99.9999th percentile
      -- but drastically increase processing time, so we cap the session path at 50 touchpoints
      , sessions_p3 as (
        with p0 as (
          select session_id
          , event_number
          , conversion
          , string_agg(
            case
              when (path_lag is null or path != path_lag) then path
            end
            , ">") over (partition by session_id order by event_number ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as session_path
          from paths
          group by session_id, event_number, path, event, conversion, path_lag
        )
        , p1 as (
          select
          session_id
          , session_path
          , conversion
          from p0
          where event_number in (select * from max_events)
          group by session_id, session_path, conversion
        )
        , p2 as (
          select *
          , case when conversion = 1 then split(session_path, ">/checkout/subscribe>")
            else null
           end as conversion_path_arr
          from p1
        )
        select
        session_id
        , session_path
        , conversion_path_arr[offset(0)] as conversion_path
        , array_length(split(conversion_path_arr[offset(0)], ",")) as conversion_path_length
        from p2
      )
      , sessions_p4 as (
        with group_user_ids as (
          select
          session_id
          , user_id
          from page_events
          group by 1,2
        )
        select
        session_id
        , string_agg(user_id) over (partition by session_id) as user_ids
        from group_user_ids
        group by session_id, user_id
      )
      , sessions_p5 as (
        with session_utms as (
          select
          session_id
          , first_value(event_number) over (partition by session_id order by event_number) as first_event
          , first_value(utm_campaign) over (partition by session_id order by event_number) as session_utm_campaign
          , first_value(utm_source) over (partition by session_id order by event_number) as session_utm_source
          , first_value(utm_medium) over (partition by session_id order by event_number) as session_utm_medium
          , first_value(utm_content) over (partition by session_id order by event_number) as session_utm_content
          , first_value(utm_term) over (partition by session_id order by event_number) as session_utm_term
          from page_events
        )
        select * from session_utms group by 1,2,3,4,5,6,7
      )
      , sessions_p6 as (
        with session_referrer as (
          select
          session_id
          , first_value(referrer_domain) over(partition by session_id order by event_number) as session_referrer
          , first_value(search) over (partition by session_id order by event_number) as session_search
          , first_value(ad_id) over (partition by session_id order by event_number) as session_ad_id
          , first_value(adset_id) over (partition by session_id order by event_number) as session_adset_id
          , first_value(campaign_id) over (partition by session_id order by event_number) as session_campaign_id
          from page_events
        )
        select * from session_referrer group by 1,2,3,4,5,6
      )
      , sessions_final as (
        select
        a.session_id
        , a.event_id
        , a.anonymous_id
        , a.ip_address
        , a.user_agent
        , a.session_start
        , a.session_end
        , a.landing_page
        , a.exit_page
        , a.conversion
        , a.bounce
        , user_ids
        , unique_pages_viewed
        , session_path
        , conversion_path
        , conversion_path_length
        , session_referrer
        , session_search
        , session_ad_id
        , session_adset_id
        , session_campaign_id
        , session_utm_campaign
        , session_utm_source
        , session_utm_medium
        , session_utm_content
        , session_utm_term
        , utm_campaign_values
        , utm_source_values
        , utm_medium_values
        , utm_content_values
        , utm_term_values
        from sessions_p0 a
        left join sessions_p1 b on a.session_id = b.session_id
        left join sessions_p2 c on a.session_id = c.session_id
        left join sessions_p3 d on a.session_id = d.session_id
        left join sessions_p4 e on a.session_id = e.session_id
        left join sessions_p5 f on a.session_id = f.session_id
        left join sessions_p6 g on a.session_id = g.session_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
      )
      select * from sessions_final where session_id is not null ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

  dimension: session_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.session_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension_group: session_start {
    type: time
    sql: ${TABLE}.session_start ;;
  }
  dimension_group: session_end {
    type: time
    sql: ${TABLE}.session_end ;;
  }
  dimension: landing_page {
    type: string
    sql: ${TABLE}.landing_page ;;
  }
  dimension: exit_page {
    type: string
    sql: ${TABLE}.exit_page ;;
  }
  dimension: conversion {
    type: yesno
    sql: ${TABLE}.conversion = 1;;
  }
  dimension: bounce {
    type: yesno
    sql: ${TABLE}.bounce = 1 ;;
  }
  dimension: user_ids {
    type: string
    sql: ${TABLE}.user_ids ;;
  }
  dimension: unique_pages_viewed {
    type: number
    sql: ${TABLE}.unique_pages_viewed ;;
  }
  dimension: session_path {
    type: string
    sql: ${TABLE}.session_path ;;
  }
  dimension: conversion_path {
    type: string
    sql: ${TABLE}.conversion_path ;;
  }
  dimension: conversion_path_length {
    type: number
    sql: ${TABLE}.conversion_path_length ;;
  }
  dimension: session_referrer {
    type: string
    sql: ${TABLE}.session_referrer ;;
  }
  dimension: session_search {
    type: string
    sql: ${TABLE}.session_search ;;
  }
  dimension: session_ad_id {
    type: string
    sql: ${TABLE}.session_ad_id ;;
  }
  dimension: session_adset_id {
    type: string
    sql: ${TABLE}.session_adset_id ;;
  }
  dimension: session_campaign_id {
    type: string
    sql: ${TABLE}.session_campaign_id ;;
  }
  dimension: session_utm_campaign {
    type: string
    sql: ${TABLE}.session_utm_campaign ;;
  }
  dimension: session_utm_source {
    type: string
    sql: ${TABLE}.session_utm_source ;;
  }
  dimension: session_utm_medium {
    type: string
    sql: ${TABLE}.session_utm_medium ;;
  }
  dimension: session_utm_content {
    type: string
    sql: ${TABLE}.session_utm_content ;;
  }
  dimension: session_utm_term {
    type: string
    sql: ${TABLE}.session_utm_term ;;
  }
  dimension: utm_campaign_values {
    type: string
    sql: ${TABLE}.utm_campaign_values ;;
  }
  dimension: utm_source_values {
    type: string
    sql: ${TABLE}.utm_source_values ;;
  }
  dimension: utm_medium_values {
    type: string
    sql: ${TABLE}.utm_medium_values ;;
  }
  dimension: utm_content_values {
    type: string
    sql: ${TABLE}.utm_content_values ;;
  }
  dimension: utm_term_values {
    type: string
    sql: ${TABLE}.utm_term_values ;;
  }
  dimension: campaign_source {
    sql: CASE
              WHEN ${TABLE}.session_utm_source IS NULL then 'Direct Traffic'
              WHEN ${TABLE}.session_utm_source = 'organic' then 'Direct Traffic'
              WHEN ${TABLE}.session_utm_source LIKE 'hs_email' then 'Internal'
              WHEN ${TABLE}.session_utm_source LIKE 'hs_automation' then 'Internal'
              WHEN ${TABLE}.session_utm_source LIKE '%site.source.name%' then 'Meta Ads'
              WHEN ${TABLE}.session_utm_source LIKE '%site_source_name%' then 'Meta Ads'
              WHEN ${TABLE}.session_utm_source = 'google_ads' then 'Google Ads'
              WHEN ${TABLE}.session_utm_source = 'GoogleAds' then 'Google Ads'
              WHEN ${TABLE}.session_utm_source = 'fb' then 'Meta Ads'
              WHEN ${TABLE}.session_utm_source = 'facebook' then 'Meta Ads'
              WHEN ${TABLE}.session_utm_source = 'ig' then 'Meta Ads'
              WHEN ${TABLE}.session_utm_source = 'bing_ads' then 'Bing Ads'
              WHEN ${TABLE}.session_utm_source = 'an' then 'Meta Ads'
              else 'other'
            END ;;
  }
  measure: total_sessions {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
  }
  measure: total_conversions {
    type: count_distinct
    sql: ${TABLE}.session_id ;;
    filters: [conversion: "yes"]
  }
  measure: conversion_rate {
    type: number
    sql: ${total_conversions}/NULLIF(${total_sessions},0) ;;
    value_format_name: percent_2
  }

  set: detail {
    fields: [
      session_id
      , anonymous_id
      , ip_address
      , user_agent
      , landing_page
      , exit_page
      , conversion
      , bounce
      , user_ids
      , unique_pages_viewed
      , session_path
      , session_utm_campaign
      , session_utm_source
      , session_utm_medium
      , session_utm_content
      , session_utm_term
      , utm_campaign_values
      , utm_source_values
      , utm_medium_values
      , utm_content_values
      , utm_term_values
    ]
  }
}
