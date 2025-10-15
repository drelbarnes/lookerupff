view: vimeo {
  derived_table: {
    sql:

      with platform as (
      select
        CAST(user_id AS VARCHAR) as user_id
        ,platform
        ,report_date
      from customers.all_customers
      where report_date >= '2025-01-01'
    ),

    customers as (
      select distinct
        CAST(customer_id AS VARCHAR)as user_id
        ,subscription_frequency as billing_period
        ,event_type
        ,date(event_occurred_at) as report_date
      FROM customers.new_customers
      where subscription_frequency != 'custom' and date(event_occurred_at) >= '2025-01-01'),

      chargebee_re_acquisition as(
      SELECT
      content_subscription_id as user_id
      ,'web' as platform
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      ,'Direct to Paid' as event_type
      ,date(received_at) as report_date

      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items like '%UP%'

      UNION ALL

      SELECT
        content_subscription_id AS user_id
        ,'web' AS platform
        ,CASE
          WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
          ELSE 'yearly'
        END AS billing_period
        ,'Direct to Paid' as event_type
        ,date(DATEADD(HOUR, -5, timestamp)) AS report_date
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'
      ),

    vimeo as (

    SELECT
      b.user_id
      ,a.platform
      ,b.billing_period
      ,b.event_type
      ,b.report_date
    FROM customers b
    LEFT JOIN platform a
    ON a.report_date = b.report_date and b.user_id = a.user_id)

    SELECT * FROM vimeo

    UNION ALL

    SELECT * FROM chargebee_re_acquisition


      ;;
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }


  dimension: user_id {
    type: string
    sql:  ${TABLE}.user_id ;;
  }

  dimension: event_type {
    type: string
    sql:  ${TABLE}.event_type ;;
  }

  dimension: platform {
    type: string
    sql:  ${TABLE}.platform ;;
  }


  measure: free_trials_gained {
    type: count_distinct
    filters: [event_type: "New Free Trial"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: trials_converted {
    type: count_distinct
    filters: [event_type: "Free Trial to Paid"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_ios {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"ios"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_tvos {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"tvos"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_android {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"android"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_android_tv {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"android_tv"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_roku {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"roku"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_amazon_fire_tv {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"amazon_fire_tv"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_amazon_fire_tablet {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"amazon_fire_tablet"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_web {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"web"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: resubscribed_vizio {
    type: count_distinct
    filters: [event_type: "Direct to Paid",platform :"vizio_tv"]
    sql: ${TABLE}.user_id  ;;
  }


}
