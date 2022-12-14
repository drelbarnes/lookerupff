view: android_app_events {
  derived_table: {
    sql:
      with app_installed as (
        select
        a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_id
        , a.context_device_advertising_id
        , b.context_aaid
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.ad_id
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.canonical_url
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from (
          select *
          from (
            select *
            , ROW_NUMBER() over (partition by id order by timestamp desc) as n
            from android.application_installed
          )
          where n = 1
        ) as a
        left join (
          select *
          from (
            select
            timestamp
            , anonymous_id
            , context_device_id
            , context_aaid
            , advertising_partner_name
            , campaign_id
            , _id as ad_id
            , ad_set_id
            , ad_set_name
            , canonical_url
            , context_campaign_source
            , context_campaign_name
            , context_campaign_medium
            , context_campaign_content
            , keyword
            , row_number() over (partition by anonymous_id, date(timestamp) order by timestamp) as n
            from android.branch_install
          )
          where n = 1
          union all
          select *
          from (
            select
            timestamp
            , anonymous_id
            , context_device_id
            , context_aaid
            , advertising_partner_name
            , campaign_id
            , _id as ad_id
            , ad_set_id
            , ad_set_name
            , canonical_url
            , context_campaign_source
            , context_campaign_name
            , context_campaign_medium
            , context_campaign_content
            , keyword
            , row_number() over (partition by anonymous_id, date(timestamp) order by timestamp) as n
            from android.branch_reinstall
          )
          where n=1
        ) as b
        on a.anonymous_id = b.anonymous_id and b.timestamp between a.timestamp and timestamp_add(a.timestamp, interval 1 minute)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
      )
      , checkout_started as (
        select
        a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_id
        , a.context_device_advertising_id
        , b.context_aaid
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.ad_id
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.canonical_url
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from (
          select *
          from (
            select *
            , row_number() over (partition by id order by timestamp desc) as n
            from android.checkout_started
          )
          where n = 1
        ) as a
        left join (
          select *
          from (
            select
            timestamp
            , anonymous_id
            , context_device_id
            , context_aaid
            , advertising_partner_name
            , campaign_id
            , _id as ad_id
            , ad_set_id
            , ad_set_name
            , canonical_url
            , context_campaign_source
            , context_campaign_name
            , context_campaign_medium
            , context_campaign_content
            , keyword
            , row_number() over (partition by anonymous_id, date(timestamp) order by timestamp) as n
            from android.branch_initiate_purchase
          )
          where n=1
        ) as b
        on a.anonymous_id = b.anonymous_id and b.timestamp between a.timestamp and timestamp_add(a.timestamp, interval 1 minute)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
      )
      , order_completed as (
        select
        a.timestamp
        , a.id
        , cast(a.user_id as string) as user_id
        , a.anonymous_id
        , a.context_device_id
        , a.context_device_advertising_id
        , b.context_aaid
        , a.context_ip
        , a.context_user_agent
        , "android" as platform
        , a.event_text as event
        , b.advertising_partner_name
        , b.ad_id
        , b.campaign_id
        , b.ad_set_id
        , b.ad_set_name
        , b.canonical_url
        , b.context_campaign_source
        , b.context_campaign_name
        , b.context_campaign_medium
        , b.context_campaign_content
        , b.keyword
        from (
          select *
          from (
            select *
            , row_number() over (partition by id order by timestamp desc) as n
            from android.order_completed
          )
          where n=1
        ) as a
        left join (
          select *
          from (
            select
            timestamp
            , anonymous_id
            , context_device_id
            , context_aaid
            , advertising_partner_name
            , campaign_id
            , _id as ad_id
            , ad_set_id
            , ad_set_name
            , canonical_url
            , context_campaign_source
            , context_campaign_name
            , context_campaign_medium
            , context_campaign_content
            , keyword
            , row_number() over (partition by anonymous_id, date(timestamp) order by timestamp) as n
            from android.branch_purchase
          )
          where n=1
        ) as b
        on a.anonymous_id = b.anonymous_id and b.timestamp between a.timestamp and timestamp_add(a.timestamp, interval 1 minute)
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
      )
      , all_events as (
      select * from app_installed
      union all
      select * from checkout_started
      union all
      select * from order_completed
      )
      , app_event_ids as (
        select *
        , to_hex(sha1(concat(id,safe_cast(timestamp as string)))) as event_id
        , row_number() over (order by timestamp) as event_number
        from all_events
      )
      , session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by anonymous_id order by event_number) as last_event
        , lead(timestamp, 1) over (partition by anonymous_id order by event_number) as next_event
        , lag(context_campaign_name,1) over (partition by anonymous_id order by event_number) as last_campaign_name
        , regexp_extract(canonical_url, r'[a-zA-Z]+\.[a-zA-Z]+\/') as referrer_domain
        , split(canonical_url, "?")[safe_offset(1)] as search
        from app_event_ids
        where anonymous_id is not null
      )
      , campaign_session_mapping_p1 as (
        select *
        , case
          when context_campaign_name is not null and (ifnull(last_campaign_name, '') != context_campaign_name) then 1
          when last_event is null or unix_seconds(timestamp) - unix_seconds(last_event) >= (7 * 24 * 60 * 60) then 1
          when context_campaign_name is null and (referrer_domain is not null) then 1
          else 0
          end as is_session_start
        from session_mapping_p0
      )
      , campaign_session_mapping_p2 as (
          select *
          , lead(is_session_start,1) over (partition by anonymous_id order by event_number) as next_session
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
        order by anonymous_id, event_number
      )
      , session_ids_p0 as (
        select *
        , case when is_session_start = 1 then to_hex(sha1(concat(event_id,safe_cast(timestamp as string))))
          else null
          end as session_id
        from campaign_session_mapping_p3
      )
      , session_ids_p1 as (
        select *
        , sum(case when is_session_start = 1 then 1 else 0 end) over (partition by anonymous_id order by event_number) as session_partition
        from session_ids_p0
      )
      , session_ids_p2 as (
        select *
        , first_value(session_id) over (partition by anonymous_id, session_partition order by event_number) as session_id_fill
        from session_ids_p1
      )
      , session_ids_p3 as (
        select
        timestamp
        , user_id
        , anonymous_id
        , coalesce(context_aaid, context_device_advertising_id) as device_id
        , coalesce(context_aaid, context_device_advertising_id) as advertising_id
        , context_ip as ip_address
        , context_user_agent as user_agent
        , platform
        , event
        , event_id
        , event_number
        , session_id_fill as session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , advertising_partner_name
        , ad_id
        , campaign_id
        , ad_set_id as adset_id
        , ad_set_name as adset_name
        , canonical_url as referrer
        , search
        , referrer_domain
        , context_campaign_source as utm_source
        , context_campaign_name as utm_campaign
        , context_campaign_medium as utm_medium
        , context_campaign_content as utm_content
        , keyword as utm_term
        from session_ids_p2
      )
      select * from session_ids_p3 order by timestamp desc ;;
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

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
  }

  dimension: event_number {
    type: string
    sql: ${TABLE}.event_number ;;
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

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension: adset_name {
    type: string
    sql: ${TABLE}.adset_name ;;
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
      advertising_id,
      ip_address,
      user_agent,
      device,
      platform,
      event,
      event_id,
      event_number,
      session_id,
      is_session_start,
      is_session_end,
      is_conversion,
      advertising_partner_name,
      ad_id,
      campaign_id,
      adset_id,
      adset_name,
      referrer,
      referrer_domain,
      search,
      utm_source,
      utm_campaign,
      utm_medium,
      utm_content,
      utm_term,
      row
    ]
  }
}
