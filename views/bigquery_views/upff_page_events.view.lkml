view: upff_page_events {
  derived_table: {
    sql: with site_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Page Viewed" as event
        , id as event_id
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(context_page_search as string) as search
        , safe_cast(title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
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
        , id as event_id
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(context_page_search as string) as search
        , safe_cast(title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.pages
      )
      , identifies as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Identify" as event
        , id as event_id
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(context_page_search as string) as search
        , safe_cast(context_page_title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , "web" as platform
        , timestamp
        from javascript.identifies
      )
      , order_completed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Order Completed" as event
        , id as event_id
        , safe_cast(context_campaign_content as string) as utm_content
        , safe_cast(context_campaign_medium as string) as utm_medium
        , safe_cast(context_campaign_name as string) as utm_campaign
        , safe_cast(context_campaign_source as string) as utm_source
        , safe_cast(context_campaign_term as string) as utm_term
        , safe_cast(context_page_referrer as string) as referrer
        , safe_cast(null as string) as search
        , safe_cast(context_page_title as string) as title
        , safe_cast(context_page_url as string) as url
        , safe_cast(context_page_path as string) as path
        , safe_cast(context_user_agent as string) as device
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.order_completed
      )
      , page_events as (
        select * from site_pages
        union all
        select * from app_pages
        union all
        select * from identifies
        union all
        select * from order_completed
      )
      , referrer_domain as (
        select *
        , regexp_extract(referrer, r'[a-zA-Z]+\.[a-zA-Z]+\/') as referrer_domain
        from page_events
      )
      -- hotfix to stitch app sessions and web sessions together during period that Vimeo OTT messed up Segment implementation
      , app_session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by user_id order by timestamp) as last_event_0
        from referrer_domain
      )
      , app_session_mapping_p1 as (
        select *
        , case
          when unix_seconds(timestamp) - unix_seconds(last_event_0) >= (60 * 30) or last_event_0 is null
            then 1
          else 0
          end as is_session_start_0
        from app_session_mapping_p0
      )
      , anon_id_mapping_p0 as (
        select
        *,
        case when event = "Identify" and is_session_start_0 = 1 then anonymous_id
          else null
          end as anon_id_2
        from app_session_mapping_p1
      )
      , anon_id_mapping_p1 as (
        select *
        , sum(case when anon_id_2 is null then 0 else 1 end) over (partition by user_id order by timestamp) as session_partition
        from anon_id_mapping_p0
        where user_id is not null
      )
      , anon_id_mapping_p2 as (
        select *
        , first_value(anon_id_2) over (partition by user_id, session_partition order by timestamp) as anon_id_alt
        from anon_id_mapping_p1
        where user_id is not null

      )
      , anon_id_mapping_p3 as (
        select a.*, b.anon_id_alt
        from app_session_mapping_p1 a
        left join anon_id_mapping_p2 b
        on a.timestamp = b.timestamp and a.anonymous_id = b.anonymous_id
      )
      , anon_id_mapping_p4 as (
        select
        timestamp
        , coalesce(anon_id_alt, anonymous_id) as anonymous_id
        , anonymous_id as anonymous_id_raw
        , user_id
        , ip_address
        , cross_domain_id
        , event
        , event_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , referrer
        , referrer_domain
        , search
        , title
        , url
        , path
        , device
        , platform
        from anon_id_mapping_p3
      )
      , session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by anonymous_id order by timestamp) as last_event
        , lag(utm_source,1) over (partition by anonymous_id order by timestamp) as last_utm_source
        , lead(timestamp, 1) over (partition by anonymous_id order by timestamp) as next_event
        from anon_id_mapping_p4
      )
      , campaign_session_mapping_p1 as (
        select *
        , case
          when last_event is null or unix_seconds(timestamp) - unix_seconds(last_event) >= (7 * 24 * 60 * 60) then 1
          when utm_source is not null and utm_source != last_utm_source then 1
          when utm_source is null and (referrer_domain is not null and referrer_domain != "upfaithandfamily.com/") then 1
          else 0
          end as is_session_start
        from session_mapping_p0
      )
      , campaign_session_mapping_p2 as (
        select *
        , lead(is_session_start,1) over (partition by anonymous_id order by timestamp) as next_session
        from campaign_session_mapping_p1
      )
      , campaign_session_mapping_p3 as (
        select *
        , case
          when next_session = 1 then 1
          when next_session is null then 1
          else 0
          end as is_session_end
        , case
          when event = "Order Completed" then 1
          else 0
          end as is_conversion
        from campaign_session_mapping_p2
      )
      , time_session_mapping_p1 as (
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
        , case
            when event = "Order Completed" then 1
            else 0
            end as is_conversion
        from session_mapping_p0
        order by anonymous_id, timestamp
      )
      , session_ids_p0 as (
        select *
        , case when is_session_start = 1 then generate_uuid()
          else null
          end as new_session_id
        from campaign_session_mapping_p3
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
        , anonymous_id_raw
        , ip_address
        , cross_domain_id
        , user_id
        , event
        , event_id
        , session_id_alt as session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , referrer
        , referrer_domain
        , search
        , title
        , url
        , path
        , device
        , platform
        from session_ids_p2
      )
      select *, row_number() over (order by timestamp) as row from session_ids_p3 ;;
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

  dimension: anonymous_id_raw {
    type: string
    sql: ${TABLE}.anonymous_id_raw ;;
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

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
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

  dimension: referrer_domain {
    type: string
    sql: ${TABLE}.referrer_domain ;;
  }

  dimension: search {
    type: string
    sql: ${TABLE}.search ;;
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

  dimension: is_session_start {
    type: yesno
    sql: ${TABLE}.is_session_start = 1 ;;
  }

  dimension: is_session_end {
    type: yesno
    sql: ${TABLE}.is_session_end = 1 ;;
  }

  dimension: is_conversion {
    type: yesno
    sql: ${TABLE}.is_conversion ;;
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
      is_session_start,
      is_session_end,
      is_conversion,
      platform,
      timestamp_time,
      row
    ]
  }
}
