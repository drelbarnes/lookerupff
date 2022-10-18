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
      select * from first_values group by 1,2,3,4,5,6,7
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
    , paths as (select session_id, event_number, path from page_events)
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
      with all_paths as (
        select session_id
        , event_number
        , string_agg(path) over (partition by session_id order by event_number ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as session_path
        from paths
        group by session_id, event_number, path
      )
      select
      session_id
      , session_path
      from all_paths
      where event_number in (select * from max_events)
      group by session_id, session_path
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
    , sessions_final as (
      select
      a.session_id
      , session_start
      , session_end
      , landing_page
      , exit_page
      , conversion
      , bounce
      , user_ids
      , touchpoints
      , session_path
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
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    )
    select * from sessions_final where session_id is not null ;;
    persist_for: "6 hours"
  }

  dimension: session_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.session_id ;;
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

  set: detail {
    fields: [
      session_id
      , landing_page
      , exit_page
      , conversion
      , bounce
      , user_ids
      , touchpoints
      , session_path
      , utm_campaign_values
      , utm_source_values
      , utm_medium_values
      , utm_content_values
      , utm_term_values
    ]
  }
}
