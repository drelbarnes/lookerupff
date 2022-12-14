view: upff_android_sessions {
  derived_table: {
    sql:
      with app_events as (
        select
        cast(timestamp as timestamp) as timestamp
        , event_number
        , event_id
        , user_id
        , anonymous_id
        , device_id
        , advertising_id
        , ip_address
        , user_agent
        , event
        , session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , referrer
        , referrer_domain
        , search
        , advertising_partner_name
        , ad_id
        , adset_id
        , adset_name
        , campaign_id
        , utm_campaign
        , utm_source
        , utm_medium
        , utm_content
        , utm_term
        , platform
        from ${android_app_events.SQL_TABLE_NAME}
      )
      , sessions_p0 as (
          with first_values as (
            select
            session_id
            , anonymous_id
            , first_value(event_id) over (partition by session_id order by event_number) as event_id
            , first_value(device_id) over (partition by session_id order by event_number) as device_id
            , first_value(advertising_id) over (partition by session_id order by event_number) as advertising_id
            , first_value(ip_address) over (partition by session_id order by event_number) as ip_address
            , first_value(user_agent) over (partition by session_id order by event_number) as user_agent
            , first_value(timestamp) over (partition by session_id order by event_number) as session_start
            , first_value(timestamp) over (partition by session_id order by event_number desc) as session_end
            , first_value(is_conversion ignore nulls) over (partition by session_id order by is_conversion desc) as conversion
            from app_events
          )
          select * from first_values group by 1,2,3,4,5,6,7,8,9,10
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
        with session_utms as (
          select
          session_id
          , first_value(utm_campaign) over (partition by session_id order by event_number) as session_utm_campaign
          , first_value(utm_source) over (partition by session_id order by event_number) as session_utm_source
          , first_value(utm_medium) over (partition by session_id order by event_number) as session_utm_medium
          , first_value(utm_content) over (partition by session_id order by event_number) as session_utm_content
          , first_value(utm_term) over (partition by session_id order by event_number) as session_utm_term
          from app_events
        )
        select * from session_utms group by 1,2,3,4,5,6
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
          from app_events
        )
        select * from session_referrer group by 1,2,3,4,5,6
      )
      , sessions_final as (
        select
        a.session_id
        , a.event_id
        , a.device_id
        , a.anonymous_id
        , a.advertising_id
        , a.ip_address
        , a.user_agent
        , session_start
        , session_end
        , conversion
        , user_ids
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
        left join sessions_p4 e on a.session_id = e.session_id
        left join sessions_p5 f on a.session_id = f.session_id
        left join sessions_p6 g on a.session_id = g.session_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
      )
      select * from sessions_final where session_id is not null ;;
    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: session_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: advertising_id {
    type: string
    sql: ${TABLE}.advertising_id ;;
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

  dimension: conversion {
    type: number
    sql: ${TABLE}.conversion ;;
  }

  dimension: user_ids {
    type: string
    sql: ${TABLE}.user_ids ;;
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

  set: detail {
    fields: [
      session_id,
      event_id,
      device_id,
      anonymous_id,
      advertising_id,
      ip_address,
      user_agent,
      session_start_time,
      session_end_time,
      conversion,
      user_ids,
      session_referrer,
      session_search,
      session_ad_id,
      session_adset_id,
      session_campaign_id,
      session_utm_campaign,
      session_utm_source,
      session_utm_medium,
      session_utm_content,
      session_utm_term,
      utm_campaign_values,
      utm_source_values,
      utm_medium_values,
      utm_content_values,
      utm_term_values
    ]
  }
}
