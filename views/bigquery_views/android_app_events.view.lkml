view: android_app_events {
  derived_table: {
    sql: with app_installed as (
        select
        a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_advertising_id as device_id
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from `up-faith-and-family-216419.android.application_installed` a
        left join (
          select
          timestamp
          , context_device_id
          , context_aaid
          , advertising_partner_name
          , campaign_id
          , ad_set_id
          , ad_set_name
          , context_campaign_source
          , context_campaign_name
          , context_campaign_medium
          , context_campaign_content
          , keyword
          from `up-faith-and-family-216419.android.branch_install`
          union all
          select
          timestamp
          , context_device_id
          , context_aaid
          , advertising_partner_name
          , campaign_id
          , ad_set_id
          , ad_set_name
          , context_campaign_source
          , context_campaign_name
          , context_campaign_medium
          , context_campaign_content
          , keyword
          from `up-faith-and-family-216419.android.branch_reinstall`
        ) as b
        on a.context_device_advertising_id = b.context_aaid and date(a.timestamp) = date(b.timestamp)
      )
      , app_opened as (
        select
          a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_advertising_id as device_id
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from `up-faith-and-family-216419.android.application_opened` a
        left join `up-faith-and-family-216419.android.branch_open` b
        on a.context_device_advertising_id = b.context_aaid and date(a.timestamp) = date(b.timestamp)
      )
      , checkout_started as (
        select
         a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_advertising_id as device_id
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from `up-faith-and-family-216419.android.checkout_started` a
        left join `up-faith-and-family-216419.android.branch_initiate_purchase` b
        on a.context_device_advertising_id = b.context_aaid and date(a.timestamp) = date(b.timestamp)
      )
      , order_completed as (
        select
         a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_advertising_id as device_id
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from `up-faith-and-family-216419.android.order_completed` a
        left join `up-faith-and-family-216419.android.branch_purchase` b
        on a.context_device_advertising_id = b.context_aaid and date(a.timestamp) = date(b.timestamp)
      )
      , all_events as (
      select * from app_installed
      union all
      select * from app_opened
      union all
      select * from checkout_started
      union all
      select * from order_completed
      )
      , session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by device_id order by timestamp) as last_event
        , lead(timestamp, 1) over (partition by device_id order by timestamp) as next_event
        from all_events
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
          end as session_id
        from session_mapping_p1
      )
      , session_ids_p1 as (
        select *
        , sum(case when session_id is null then 0 else 1 end) over (partition by device_id order by timestamp) as session_partition
        from session_ids_p0
      )
      , session_ids_p2 as (
        select *
        , first_value(session_id) over (partition by device_id, session_partition order by timestamp) as session_id_fill
        from session_ids_p1
      )
      , session_ids_p3 as (
        select
        timestamp
        , id
        , user_id
        , anonymous_id
        , device_id
        , context_ip as ip_address
        , context_user_agent as device
        , platform
        , event
        , session_id_fill as session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , advertising_partner_name
        , campaign_id
        , ad_set_id
        , ad_set_name
        , context_campaign_source as utm_source
        , context_campaign_name as utm_campaign
        , context_campaign_medium as utm_medium
        , context_campaign_content as utm_content
        , keyword as utm_term
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: device_id {
    type: string
    sql: ${TABLE}.device_id ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: device {
    type: string
    sql: ${TABLE}.device ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: is_session_start {
    type: number
    sql: ${TABLE}.is_session_start ;;
  }

  dimension: is_session_end {
    type: number
    sql: ${TABLE}.is_session_end ;;
  }

  dimension: is_conversion {
    type: number
    sql: ${TABLE}.is_conversion ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: ad_set_id {
    type: string
    sql: ${TABLE}.ad_set_id ;;
  }

  dimension: ad_set_name {
    type: string
    sql: ${TABLE}.ad_set_name ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
  }

  dimension: row {
    type: number
    sql: ${TABLE}.row ;;
  }

  set: detail {
    fields: [
      timestamp_time,
      id,
      user_id,
      anonymous_id,
      device_id,
      ip_address,
      device,
      platform,
      event,
      session_id,
      is_session_start,
      is_session_end,
      is_conversion,
      advertising_partner_name,
      campaign_id,
      ad_set_id,
      ad_set_name,
      utm_source,
      utm_campaign,
      utm_medium,
      utm_content,
      utm_term,
      row
    ]
  }
}
