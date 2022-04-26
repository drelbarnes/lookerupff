view: identity_resolution {
  derived_table: {
    sql: /*
      /*
      /* Established initial query – UP Faith & Family Marketing Site
      /*
      */

      with upff_home_page as (
        select
        timestamp
        , user_id
        , anonymous_id
        , context_ip
        , context_traits_cross_domain_id
        , 'website_visit' as event
        , context_campaign_content as utm_content
        , context_campaign_medium as utm_medium
        , context_campaign_name as utm_campaign
        , context_campaign_source as utm_source
        , context_campaign_term as utm_term
        , context_page_referrer as referrer
        , title as view
        , context_user_agent as user_agent
        from javascript_upff_home.pages
        where
        timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY)
        and
        timestamp < TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY), INTERVAL {% parameter traffic_window %} DAY)
      ),
      /*
      /*
      /* UP Faith & Family Seller Site
      /*
      */

      upff_seller_page as (
          select
        timestamp
        , user_id
        , anonymous_id
        , context_ip
        , context_traits_cross_domain_id
        , 'website_visit' as event
        , context_campaign_content as utm_content
        , context_campaign_medium as utm_medium
        , context_campaign_name as utm_campaign
        , context_campaign_source as utm_source
        , context_campaign_term as utm_term
        , context_page_referrer as referrer
        , view
        , context_user_agent as user_agent
        from javascript.pages
        where
        timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY)
        and
        timestamp < TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY), INTERVAL {% parameter traffic_window %} DAY)
      ),
      /*
      /*
      /* UP Faith & Family Seller Site Order Completed Event
      /*
      */

      web_order_completed as (
        select
        timestamp
        , user_id as user_id
        , anonymous_id
        , context_ip
        , context_traits_cross_domain_id
        , 'order_completed' as event
        , context_campaign_content as utm_content
        , context_campaign_medium as utm_medium
        , context_campaign_name as utm_campaign
        , context_campaign_source as utm_source
        , context_campaign_term as utm_term
        , context_page_referrer as referrer
        , view
        , context_user_agent as user_agent
        from javascript.order_completed
        where
        timestamp >= TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY)
        and
        timestamp < TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -({% parameter traffic_window %} - 1) DAY), INTERVAL {% parameter traffic_window %} DAY)
      ),

      together as (
      SELECT * FROM upff_home_page
      UNION ALL
      SELECT * FROM upff_seller_page
      UNION ALL
      SELECT * FROM web_order_completed
      )

      select * FROM together
      ;;

  }

  parameter: traffic_window {
    type: unquoted
    label: "Traffic Window"
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

    allowed_value: {
      label: "180 days"
      value: "180"
    }

    allowed_value: {
      label: "365 days"
      value: "365"
    }
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
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

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_term {
    type: string
    sql: ${TABLE}.utm_term ;;
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  measure: distinct_count {
    type: count_distinct
    sql:${anonymous_id} ;;
    drill_fields: [detail*]
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  set: detail {
    fields: [
      anonymous_id,
      timestamp_time,
      user_id,
      context_ip,
      context_traits_cross_domain_id,
      event,
      utm_source,
      utm_medium,
      utm_campaign,
      utm_term,
      utm_content,
      view,
      referrer,
      user_agent,
    ]
  }
}
