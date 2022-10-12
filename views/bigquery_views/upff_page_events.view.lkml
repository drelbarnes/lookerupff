view: upff_page_events {
  derived_table: {
    sql: with site_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Page Viewed" as event
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , "" as session_id
        , "web" as platform
        , timestamp
        from javascript_upff_home.pages
      )
      , app_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Page Viewed" as event
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , safe_cast(session_id as string) as session_id
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.pages
      )
      , page_events as (
        select * FROM site_pages
        union all
        select * from app_pages
      )
      , session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by anonymous_id order by timestamp) as last_event
        , lead(timestamp, 1) over (partition by anonymous_id order by timestamp) as next_event
        from page_events
      )
      , session_mapping_p1 as (
        select *
        , case
          when unix_seconds(timestamp) - unix_seconds(last_event) >= (60 * 30) or last_event is null
            then 1
          else 0
          end as is_session_start
        , case
          when unix_seconds(next_event) - unix_seconds(timestamp) >= (60 * 30) or next_event is null
            then 1
          else 0
          end as is_session_end
        from session_mapping_p0
        order by anonymous_id, timestamp
      )
      , session_ids_p0 as (
        select *
        , case when is_session_start = 1 then generate_uuid()
          else null
          end as new_session_id
        from session_mapping_p0
      )
      , session_ids_p1 as (
        select *
        , sum(case when new_session_id is null then 0 else 1 end) over (partition by anonymous_id order by timestamp) as session_partition
        from session_ids_p0
      )
      , session_ids_p2 as (
      select *
      , first_value(new_session_id) over (partition by anonymous_id, session_partition order by timestamp) as session_id_alt
      from session_ids_p1
      )
      , session_ids_p3 as (
        select
        timestamp
        , anonymous_id
        , ip_address
        , cross_domain_id
        , user_id
        , event
        , coalesce(session_id_alt, session_id) as session_id
        , session_id as app_session_id
        , is_session_start
        , is_session_end
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , referrer
        , title
        , url
        , path
        , device
        , platform
        from session_ids_p2
      )
      select *, row_number() over (order by timestamp) as row from session_ids_p3
       ;;

    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
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

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: app_session_id {
    type: string
    sql: ${TABLE}.app_session_id ;;
  }

  dimension: is_session_start {
    type: yesno
    sql: ${TABLE}.is_session_start ;;
  }

  dimension: is_session_end {
    type: yesno
    sql: ${TABLE}.is_session_end ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
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
      title,
      url,
      path,
      device,
      session_id,
      app_session_id,
      is_session_start,
      is_session_end,
      platform,
      timestamp_time,
      row
    ]
  }
}
