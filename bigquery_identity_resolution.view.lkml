view: bigquery_identity_resolution {
  derived_table: {
    sql: /*
      /*
      /* Established initial query – UP Faith & Family Marketing Site
      /*
      */

      with upff_home_page as (SELECT anonymous_id
      , uuid_ts
      , user_id
      , context_ip
      , context_traits_cross_domain_id
      , context_campaign_source as utm_source
      , context_campaign_medium as utm_medium
      , context_campaign_name as utm_name
      , context_campaign_id utm_id
      , context_campaign_content utm_content
      , 'website_visit' as event
      , title as view
      , context_page_referrer as referrer
      , context_user_agent as user_agent
      , received_at
      , timestamp
      FROM javascript_upff_home.pages),

      /*
      /*
      /* UP Faith & Family Seller Site
      /*
      */

      upff_seller_page as (SELECT anonymous_id
      , uuid_ts
      , user_id
      , context_ip
      , context_traits_cross_domain_id
      , context_campaign_source as utm_source
      , context_campaign_medium as utm_medium
      , context_campaign_name as utm_name
      , context_campaign_id utm_id
      , context_campaign_content utm_content
      , 'website_visit' as event
      , video_title as view
      , context_page_referrer as referrer
      , context_user_agent as user_agent
      , received_at
      , timestamp
      FROM javascript.pages),

      /*
      /*
      /* UP Faith & Family Seller Site Order Completed Event
      /*
      */

      upff_order_completed as (SELECT anonymous_id
      , uuid_ts
      , user_id
      , context_ip
      , context_traits_cross_domain_id
      , context_campaign_source as utm_source
      , context_campaign_medium as utm_medium
      , context_campaign_name as utm_name
      , '' as utm_id
      , context_campaign_content as utm_content
      , 'order_completed' as event
      , view
      , context_page_referrer as referrer
      , context_user_agent as user_agent
      , received_at
      , timestamp
      FROM javascript.order_completed),

      together as (
      SELECT * FROM upff_home_page
      UNION ALL
      SELECT * FROM upff_seller_page
      UNION ALL
      SELECT * FROM upff_order_completed
      ),

      a as (
      select *, row_number() over
        (partition by user_id order by received_at {% parameter attribution_method %}) as row FROM together
      )

      select * FROM a
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


  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
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
