view: upff_web_sessions {
  derived_table: {
    sql: with page_events as (
        select
        cast(timestamp as timestamp) as timestamp
        , row as event_number
        , anonymous_id
        , ip_address
        , cross_domain_id
        , user_id
        , event
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
        , title
        , url
        , path
        , device
        , platform
        from ${upff_page_events.SQL_TABLE_NAME}
      )
      , sessions_p0 as (
        with first_values as (
          select
          session_id
          , anonymous_id
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
        select * from first_values group by 1,2,3,4,5,6,7,8
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
        select
        session_id
        , event_number
        , path
        , event
        , first_value(is_conversion ignore nulls) over (partition by session_id order by is_conversion desc) as conversion
        from page_events
        group by session_id, event_number, path, event, is_conversion
      )
      , sessions_p2 as (
        select
        session_id
        , count(path) over (partition by session_id) as touchpoints
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
          , string_agg(case when event = "Page Viewed" or event = "Order Completed" then path end) over (partition by session_id order by event_number ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as session_path
          from paths
          group by session_id, event_number, path, event, conversion
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
          , case when conversion = 1 then split(session_path, ",/checkout/subscribe,")
            else null
           end as conversion_path_arr
          from p1
        )
        select
        session_id
        , session_path
        , conversion_path_arr[offset(0)] as conversion_path
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
        with first_utms as (
          select
          session_id
          , first_value(event_number) over (partition by session_id order by event_number) as first_event
          , first_value(utm_campaign) over (partition by session_id order by event_number) as first_utm_campaign
          , first_value(utm_source) over (partition by session_id order by event_number) as first_utm_source
          , first_value(utm_medium) over (partition by session_id order by event_number) as first_utm_medium
          , first_value(utm_content) over (partition by session_id order by event_number) as first_utm_content
          , first_value(utm_term) over (partition by session_id order by event_number) as first_utm_term
          from page_events
        )
        select * from first_utms group by 1,2,3,4,5,6,7
      )
      , sessions_final as (
        select
        a.session_id
        , a.anonymous_id
        , session_start
        , session_end
        , landing_page
        , exit_page
        , conversion
        , bounce
        , user_ids
        , touchpoints
        , session_path
        , conversion_path
        , first_utm_campaign
        , first_utm_source
        , first_utm_medium
        , first_utm_content
        , first_utm_term
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
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
      )
    select * from sessions_final where session_id is not null ;;
    persist_for: "6 hours"
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
  dimension: touchpoints {
    type: number
    sql: ${TABLE}.touchpoints ;;
  }
  dimension: session_path {
    type: string
    sql: ${TABLE}.session_path ;;
  }
  dimension: conversion_path {
    type: string
    sql: ${TABLE}.conversion_path ;;
  }
  dimension: first_utm_campaign {
    type: string
    sql: ${TABLE}.first_utm_campaign ;;
  }
  dimension: first_utm_source {
    type: string
    sql: ${TABLE}.first_utm_source ;;
  }
  dimension: first_utm_medium {
    type: string
    sql: ${TABLE}.first_utm_medium ;;
  }
  dimension: first_utm_content {
    type: string
    sql: ${TABLE}.first_utm_content ;;
  }
  dimension: first_utm_term {
    type: string
    sql: ${TABLE}.first_utm_term ;;
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
              WHEN ${TABLE}.first_utm_source IS NULL then 'Direct Traffic'
              WHEN ${TABLE}.first_utm_source = 'organic' then 'Direct Traffic'
              WHEN ${TABLE}.first_utm_source LIKE 'hs_email' then 'Internal'
              WHEN ${TABLE}.first_utm_source LIKE 'hs_automation' then 'Internal'
              WHEN ${TABLE}.first_utm_source LIKE '%site.source.name%' then 'Meta Ads'
              WHEN ${TABLE}.first_utm_source LIKE '%site_source_name%' then 'Meta Ads'
              WHEN ${TABLE}.first_utm_source = 'google_ads' then 'Google Ads'
              WHEN ${TABLE}.first_utm_source = 'GoogleAds' then 'Google Ads'
              WHEN ${TABLE}.first_utm_source = 'fb' then 'Meta Ads'
              WHEN ${TABLE}.first_utm_source = 'facebook' then 'Meta Ads'
              WHEN ${TABLE}.first_utm_source = 'ig' then 'Meta Ads'
              WHEN ${TABLE}.first_utm_source = 'bing_ads' then 'Bing Ads'
              WHEN ${TABLE}.first_utm_source = 'an' then 'Meta Ads'
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
      , landing_page
      , exit_page
      , conversion
      , bounce
      , user_ids
      , touchpoints
      , session_path
      , first_utm_campaign
      , first_utm_source
      , first_utm_medium
      , first_utm_content
      , first_utm_term
      , utm_campaign_values
      , utm_source_values
      , utm_medium_values
      , utm_content_values
      , utm_term_values
    ]
  }
}
