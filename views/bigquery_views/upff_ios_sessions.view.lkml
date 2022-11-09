view: upff_ios_sessions {
  derived_table: {
    sql: with app_events as (
      select
      cast(timestamp as timestamp) as timestamp
      , row as event_number
      , user_id
      , anonymous_id
      , device_id
      , ip_address
      , device
      , platform
      , event
      , session_id
      , is_session_start
      , is_session_end
      , is_conversion
      , utm_campaign
      , advertising_partner_name
      , campaign_id
      , ad_set_id
      , ad_set_name
      , utm_source
      , utm_medium
      , utm_content
      , utm_term
      from ${ios_app_events.SQL_TABLE_NAME}
    )
    , sessions_p0 as (
      with first_values as (
        select
        session_id
        , device_id
        , first_value(timestamp) over (partition by session_id order by event_number) as session_start
        , first_value(timestamp) over (partition by session_id order by event_number desc) as session_end
        , first_value(is_conversion ignore nulls) over (partition by session_id order by is_conversion desc) as conversion
        from app_events
      )
      select * from first_values group by 1,2,3,4,5
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
        from app_events
        group by session_id, utm_campaign, utm_source, utm_medium, utm_content, utm_term
      )
      select * from sessions_utm_values group by 1,2,3,4,5,6
    )
    , sessions_p4 as (
      with group_user_ids as (
        select
        session_id
        , user_id
        from app_events
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
        from app_events
      )
      select * from first_utms group by 1,2,3,4,5,6,7
    )
    , sessions_final as (
      select
      a.session_id
      , a.device_id
      , session_start
      , session_end
      , conversion
      , user_ids
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
      left join sessions_p4 e on a.session_id = e.session_id
      left join sessions_p5 f on a.session_id = f.session_id
      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    )
    select * from sessions_final where session_id is not null
     ;;
    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension_group: session_start {
    type: time
    sql: ${TABLE}.session_start ;;
  }

  dimension_group: session_end {
    type: time
    sql: ${TABLE}.session_end ;;
  }

  dimension: conversion {
    type: number
    sql: ${TABLE}.conversion ;;
  }

  dimension: user_ids {
    type: string
    sql: ${TABLE}.user_ids ;;
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

  set: detail {
    fields: [
      session_id,
      device_id,
      session_start_time,
      session_end_time,
      conversion,
      user_ids,
      first_utm_campaign,
      first_utm_source,
      first_utm_medium,
      first_utm_content,
      first_utm_term,
      utm_campaign_values,
      utm_source_values,
      utm_medium_values,
      utm_content_values,
      utm_term_values
    ]
  }
}
