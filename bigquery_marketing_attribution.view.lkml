view: bigquery_marketing_attribution{
  derived_table: {

    sql:

      /*
      /* Established initial query match between UP Faith & Family Marketing & Seller Site
      /*d
      */

     with joined_sites as (
        SELECT  j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.context_campaign_source as utm_source
      , j.context_campaign_medium as utm_medium
      , j.context_campaign_name as utm_name
      , '' as utm_id
      , j.context_campaign_content as utm_content
      , 'site_matches' as event
      , p.view
      , p.context_page_referrer as referrer
      , 'web' as platform
      , '' as context_revenue
      , j.received_at
      , p.timestamp
        FROM javascript_upff_home.pages AS j
        INNER JOIN  javascript.pages AS p
        ON j.context_ip = p.context_ip
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))

      ),

      joined_sites_conversion as (
        SELECT j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.utm_source
      , j.utm_medium
      , j.utm_name
      , '' as utm_id
      , j.utm_content
      , 'converisons' as event
      , p.view
      , p.referrer
      , p.platform
      , p.context_revenue as revenue
      , j.received_at
      , p.timestamp
        FROM  joined_sites AS j
        INNER JOIN javascript.order_completed AS p
        ON j.context_ip = p.context_ip
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))


      ),

      joined_sites_anonymous as (
        SELECT  j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.context_campaign_source as utm_source
      , j.context_campaign_medium as utm_medium
      , j.context_campaign_name as utm_name
      , '' as utm_id
      , j.context_campaign_content as utm_content
      , 'site_matches' as event
      , p.view
      , p.context_page_referrer as referrer
      , 'web' as platform
      , '' as context_revenue
      , j.received_at
      , p.timestamp
        FROM javascript_upff_home.pages AS j
        INNER JOIN  javascript.pages AS p
        ON j.anonymous_id = p.anonymous_id
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))

      ),

      joined_sites_conversion_anonymous as (
        SELECT j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.utm_source
      , j.utm_medium
      , j.utm_name
      , '' as utm_id
      , j.utm_content
      , 'converisons' as event
      , p.view
      , p.referrer
      , p.platform
      , p.context_revenue as revenue
      , j.received_at
      , p.timestamp
        FROM  joined_sites_anonymous AS j
        INNER JOIN javascript.order_completed AS p
        ON j.anonymous_id = p.anonymous_id
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))

      ),

      joined_sites_cross_domain_id as (
        SELECT  j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.context_campaign_source as utm_source
      , j.context_campaign_medium as utm_medium
      , j.context_campaign_name as utm_name
      , '' as utm_id
      , j.context_campaign_content as utm_content
      , 'site_matches' as event
      , p.view
      , p.context_page_referrer as referrer
      , 'web' as platform
      , '' as context_revenue
      , j.received_at
      , p.timestamp
        FROM javascript_upff_home.pages AS j
        INNER JOIN  javascript.pages AS p
        ON j.context_traits_cross_domain_id = p.context_traits_cross_domain_id
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))

      ),

      joined_sites_conversion_cross_domain as (
        SELECT j.anonymous_id
      , j.uuid_ts
      , p.user_id
      , p.context_ip
      , p.context_traits_cross_domain_id
      , j.utm_source
      , j.utm_medium
      , j.utm_name
      , '' as utm_id
      , j.utm_content
      , 'converisons' as event
      , p.view
      , p.referrer
      , p.platform
      , p.context_revenue as revenue
      , j.received_at
      , p.timestamp
        FROM  joined_sites_cross_domain_id AS j
        INNER JOIN javascript.order_completed AS p
        ON j.context_traits_cross_domain_id = p.context_traits_cross_domain_id
        WHERE ((( p.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY))) AND
( p.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter attribution_window %} - 1) DAY), INTERVAL {% parameter attribution_window %} DAY)))))

      ),

      join_all_sources as (

        SELECT * FROM joined_sites_conversion
        UNION ALL
        SELECT * FROM joined_sites_conversion_anonymous
        UNION ALL
        SELECT * FROM joined_sites_conversion_cross_domain

      ),


      distinct_conversions as (
      select distinct * FROM join_all_sources
      ),

      all_conversions as (select *, row_number() over
        (partition by user_id order by received_at {% parameter attribution_method %}) as row FROM distinct_conversions)


      select distinct *  FROM all_conversions
       ;;


    }



    parameter: attribution_method {
      type: unquoted
      label: "Attribution Method"
      allowed_value: {
        label: "First Touch Attribution"
        value: "asc"
      }
      allowed_value: {
        label: "Last Touch Attribution"
        value: "desc"
      }
    }

    parameter: attribution_window {
      type: unquoted
      label: "Attribution Window"
      allowed_value: {
        label: "7 days"
        value: "7"
      }
      allowed_value: {
        label: "14 days"
        value: "14"
      }
      allowed_value: {
        label: "28 days"
        value: "28"
      }
      allowed_value: {
        label: "30 days"
        value: "30"
      }
      allowed_value: {
        label: "60 days"
        value: "60"
      }
      allowed_value: {
        label: "90 days"
        value: "90"
      }
    }

    dimension: anonymous_id {
      type: string
      sql: ${TABLE}.anonymous_id ;;
    }

    dimension_group: uuid_ts {
      type: time
      sql: ${TABLE}.uuid_ts ;;
    }

    dimension: user_id {
      type: string
      primary_key: yes
      sql: ${TABLE}.user_id ;;
    }


    dimension: context_ip {
      type: string
      sql: ${TABLE}.context_ip ;;
    }

    dimension: context_traits_cross_domain_id {
      type: string
      sql: ${TABLE}.context_traits_cross_domain_id ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: revenue {
      type: string
      sql: ${TABLE}.revenue ;;
    }

    dimension: plan_type {
      sql: CASE
              WHEN ${TABLE}.revenue = 53.99 then 'yearly'
              WHEN ${TABLE}.revenue = 5.99 then 'monthly'
          END ;;
    }

    dimension: utm_source {
      type: string
      sql: ${TABLE}.utm_source ;;
    }

    dimension: campaign_source {
      sql: CASE
              WHEN ${TABLE}.utm_source IS NULL then 'Organic'
              WHEN ${TABLE}.utm_source LIKE '%site.source.name%' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source LIKE '%site_source_name%' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'google_ads' then 'Google Ads'
              WHEN ${TABLE}.utm_source = 'GoogleAds' then 'Google Ads'
              WHEN ${TABLE}.utm_source = 'fb' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'ig' then 'Facebook Ads'
              WHEN ${TABLE}.utm_source = 'bing_ads' then 'Bing Ads'
              else ${TABLE}.utm_source
            END ;;
    }


    dimension: utm_medium {
      type: string
      sql: ${TABLE}.utm_medium ;;
    }

    dimension: utm_name {
      type: string
      sql: ${TABLE}.utm_name ;;
    }

    dimension: utm_id {
      type: string
      sql: ${TABLE}.utm_id ;;
    }

    dimension: utm_content {
      type: string
      sql: ${TABLE}.utm_content ;;
    }

    dimension: event {
      type: string
      sql: ${TABLE}.event ;;
    }

    dimension: view {
      type: string
      sql: ${TABLE}.view ;;
    }

    dimension: referrer {
      type: string
      sql: ${TABLE}.referrer ;;
    }

    dimension: user_agent {
      type: string
      sql: ${TABLE}.user_agent ;;
    }

    dimension_group: received_at {
      type: time
      sql: ${TABLE}.received_at ;;
    }

    dimension: row {
      type: number
      sql: ${TABLE}.row;;

    }

    dimension_group: timestamp {
      type: time
      sql: ${TABLE}.timestamp ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    measure: distinct_count {
      type: count_distinct
      sql: ${user_id};;
    }

    measure: distinct_facebook_count {
      type: count_distinct
      sql:CASE
          WHEN ${TABLE}.utm_source = 'fb' THEN ${user_id}
          WHEN ${TABLE}.utm_source = 'ig' THEN ${user_id}
          WHEN ${TABLE}.utm_source LIKE '%site.source.name%' then ${user_id}
          WHEN ${TABLE}.utm_source LIKE '%site_source_name%' then ${user_id}
    END ;;
    }

    measure: distinct_google_count {
      type: count_distinct
      sql: ${user_id};;
      filters: [utm_source: "google_ads"]
    }

    measure: distinct_bing_count {
      type: count_distinct
      sql: ${user_id};;
      filters: [utm_source: "bing_ads"]
    }

    measure: distinct_organic_count {
      type: count_distinct
      sql:CASE
          WHEN ${TABLE}.utm_source IS NULL THEN ${user_id}
       END ;;
    }

    set: detail {
      fields: [
        anonymous_id,
        uuid_ts_time,
        user_id,
        context_ip,
        context_traits_cross_domain_id,
        utm_source,
        utm_medium,
        utm_name,
        utm_id,
        utm_content,
        event,
        view,
        referrer,
        user_agent,
        received_at_time,
        timestamp_time
      ]
    }

  }
