view: upff_page_events {
  derived_table: {
    sql: with site_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Page Viewed" as event
        , id
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
        , safe_cast(context_user_agent as string) as user_agent
        , "web" as platform
        , timestamp
        from javascript_upff_home.pages
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
      )
      , app_pages as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Page Viewed" as event
        , id
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
        , safe_cast(context_user_agent as string) as user_agent
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.pages
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
      )
      , identifies as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Identify" as event
        , id
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
        , safe_cast(context_user_agent as string) as user_agent
        , "web" as platform
        , timestamp
        from javascript.identifies
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
      )
      , order_completed as (
        select
        safe_cast(user_id as string) as user_id
        , safe_cast(anonymous_id as string) as anonymous_id
        , safe_cast(context_ip as string) as ip_address
        , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
        , "Order Completed" as event
        , id
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
        , safe_cast(context_user_agent as string) as user_agent
        , safe_cast(platform as string) as platform
        , timestamp
        from javascript.order_completed
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
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
      -- DATA CLEANING AND PROCESSING
      -- unique event_id creation
      , page_event_ids as (
        select *
        , to_hex(sha1(concat(id,safe_cast(timestamp as string)))) as event_id
        from page_events
      )
      , referrer_domain_p0 as (
        select event_id
        , regexp_extract(referrer, r'[a-zA-Z]+\.[a-zA-Z]+\/') as referrer_domain
        from page_event_ids
      )
      , search_params_p0 as (
        select event_id
        , split(ltrim(search, "?"), "&") as search_params
        from page_event_ids
      )
      , search_params_p1 as (
        SELECT event_id, flattened_params
        FROM search_params_p0 CROSS JOIN search_params_p0.search_params AS flattened_params
      )
      , search_params_p2 as (
        select event_id
        , split(flattened_params, "=") as parameters
        from search_params_p1
      )
      , search_params_p3 as (
        SELECT event_id, parameters[safe_offset(0)] as key, parameters[safe_offset(1)] as value
        from search_params_p2
      )
      , search_params_p4 as (
        select *
        from search_params_p3
        PIVOT(string_agg(value) FOR key IN ("ad_id", "adset_id", "campaign_id"))
      )
      , clean_page_events as (
        select a.*
        , b.referrer_domain
        , c.ad_id
        , c.adset_id
        , c.campaign_id
        from page_event_ids as a
        left join referrer_domain_p0 as b
        on a.event_id = b.event_id
        left join search_params_p4 as c
        on a.event_id = c.event_id
      )
      -- THIS BLOCK FIXS THE ANONYMOUS ID BUG OF Q222-Q322
      -- hotfix to stitch app sessions and web sessions together during period that Vimeo OTT messed up Segment anon_id implementation
      , app_session_mapping_p0 as (
        select *
        , lag(timestamp,1) over (partition by user_id order by timestamp) as last_event_0
        from clean_page_events
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
        on a.event_id = b.event_id
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
        , referrer
        , search
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , title
        , url
        , path
        , user_agent
        , platform
        from anon_id_mapping_p3
        where event in ("Page Viewed","Order Completed")
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
      )
      -- END OF BLOCK
      , session_mapping_p0 as (
        select *
        , row_number() over (order by timestamp) as event_number
        from anon_id_mapping_p4
      )
      , session_mapping_p1 as (
        select *
        , lag(timestamp,1) over (partition by anonymous_id order by event_number) as last_event
        , lag(utm_campaign,1) over (partition by anonymous_id order by event_number) as last_utm_campaign
        , lead(timestamp, 1) over (partition by anonymous_id order by event_number) as next_event
        from session_mapping_p0
      )
      , campaign_session_mapping_p1 as (
        select *
        , case
          when utm_campaign is not null and (ifnull(last_utm_campaign, '') != utm_campaign) then 1
          when last_event is null or unix_seconds(timestamp) - unix_seconds(last_event) >= (7 * 24 * 60 * 60) then 1
          when utm_campaign is null and (referrer_domain is not null and referrer_domain != "upfaithandfamily.com/") then 1
          else 0
          end as is_session_start
        from session_mapping_p1
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
        from session_mapping_p1
        order by anonymous_id, timestamp
      )
      , session_ids_p0 as (
        select *
        , case when is_session_start = 1 then to_hex(sha1(concat(event_id,safe_cast(timestamp as string))))
          else null
          end as new_session_id
        from campaign_session_mapping_p3
      )
      , session_ids_p1 as (
        select *
        , sum(case when new_session_id is null then 0 else 1 end) over (partition by anonymous_id order by event_number) as session_partition
        from session_ids_p0
      )
      , session_ids_p2 as (
        select *
        , first_value(new_session_id) over (partition by anonymous_id, session_partition order by event_number) as session_id_alt
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
        , event_number
        , session_id_alt as session_id
        , is_session_start
        , is_session_end
        , is_conversion
        , referrer
        , search
        , referrer_domain
        , ad_id
        , adset_id
        , campaign_id
        , utm_content
        , utm_medium
        , utm_campaign
        , utm_source
        , utm_term
        , title
        , url
        , path
        , user_agent
        , platform
        from session_ids_p2
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
      )
      select * from session_ids_p3;;
    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: event_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.event_id ;;
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

  dimension: event_number {
    type: number
    sql: ${TABLE}.event_number ;;
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

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: adset_id {
    type: string
    sql: ${TABLE}.adset_id ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
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

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
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

  dimension: campaign_source {
    sql: CASE
      WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is null or ${TABLE}.referrer_domain = "upfaithandfamily.com/" then "unknown"
      WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is not null and ${TABLE}.referrer_domain != "upfaithandfamily.com/" then ${TABLE}.referrer_domain
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
      title,
      url,
      path,
      user_agent,
      session_id,
      is_session_start,
      is_session_end,
      is_conversion,
      platform,
      timestamp_time,
      event_number
    ]
  }
}
