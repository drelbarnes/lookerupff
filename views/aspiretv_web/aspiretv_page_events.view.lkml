view: aspiretv_page_events {
    derived_table: {
      sql: CREATE TEMP FUNCTION URLDECODE(url STRING) AS ((
        SELECT SAFE_CONVERT_BYTES_TO_STRING(
          ARRAY_TO_STRING(ARRAY_AGG(
              IF(STARTS_WITH(y, '%'), FROM_HEX(SUBSTR(y, 2)), CAST(y AS BYTES)) ORDER BY i
            ), b''))
        FROM UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}|[^%]+")) AS y WITH OFFSET AS i
      ));
      with site_pages as (
        select *
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_campaign=([^&]+)'), '+', ' ') AS utm_campaign
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_source=([^&]+)'), '+', ' ') AS utm_source
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_medium=([^&]+)'), '+', ' ') AS utm_medium
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_content=([^&]+)'), '+', ' ') AS utm_content
        , REPLACE(REGEXP_EXTRACT(search, '(?i)utm_term=([^&]+)'), '+', ' ') AS utm_term
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)ad_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_ad=([^&]+)'), '+', ' ')) AS ad_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)adset_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_grp=([^&]+)'), '+', ' ')) AS adset_id
        , coalesce(REPLACE(REGEXP_EXTRACT(search, '(?i)campaign_id=([^&]+)'), '+', ' '), REPLACE(REGEXP_EXTRACT(search, '(?i)hsa_cam=([^&]+)'), '+', ' ')) AS campaign_id
        from (
          select
          id
          , timestamp
          , safe_cast(user_id as string) as user_id
          , safe_cast(anonymous_id as string) as anonymous_id
          , safe_cast(context_ip as string) as ip_address
          , safe_cast(context_traits_cross_domain_id as string) as cross_domain_id
          , safe_cast(context_user_agent as string) as user_agent
          , "Page Viewed" as event
          , "web" as platform
          , safe_cast(context_page_url as string) as url
          , URLDECODE(REGEXP_EXTRACT(safe_cast(context_page_url as string), 'Error! Hyperlink reference not valid.')) as search
          , safe_cast(context_page_referrer as string) as referrer
          , NET.REG_DOMAIN(safe_cast(context_page_referrer as string)) AS referrer_domain
          , safe_cast(context_page_title as string) as title
          , safe_cast(context_page_path as string) as path
          from `up-faith-and-family-216419.javascript_aspire_tv.pages`
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        )
      )
      -- DATA CLEANING AND PROCESSING
      -- unique event_id creation
      , page_event_ids_p0 as (
        select *
        , to_hex(sha1(concat(id,safe_cast(timestamp as string)))) as event_id
        from site_pages
      )
      , page_event_ids_p1 as (
        select
        timestamp
        , anonymous_id
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
        from page_event_ids_p0
        where event in ("Page Viewed","Order Completed")
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
      )
      -- END OF BLOCK
      , session_mapping_p0 as (
        select *
        , row_number() over (order by timestamp) as event_number
        from page_event_ids_p1
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
          when utm_campaign is null and (referrer_domain is not null and referrer_domain != "aspire.tv") then 1
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
          -- TODO: figure out conversion mapping for linear
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
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
        )
        select * from session_ids_p3;;
      datagroup_trigger: upff_daily_refresh_datagroup
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
              WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is null or ${TABLE}.referrer_domain = "aspire.tv/" then "unknown"
              WHEN ${TABLE}.utm_source is null and ${TABLE}.referrer_domain is not null and ${TABLE}.referrer_domain != "aspire.tv/" then ${TABLE}.referrer_domain
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
