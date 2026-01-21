view: sub_count {
  derived_table: {
    sql:
    WITH active as(
      SELECT
        report_date
        ,user_id
        ,CASE
          WHEN platform = 'Chargebee' THEN 'web'
          ELSE platform
        END AS platform
        ,billing_period
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE status in ( 'active','non_renewing','enabled')
        ),

      trial as (
      SELECT
      report_date
      ,user_id
      ,platform
      ,billing_period
      FROM ${free_trials.SQL_TABLE_NAME}
      ),

      active_ios as (
      SELECT * FROM ${ios.SQL_TABLE_NAME}
      ),

      active_count_pre as (
      SELECT
      count(distinct user_id) as user_count
      ,report_date
      ,platform
      ,billing_period
      FROM active
      WHERE platform not in ('ios')
      GROUP BY 2,3,4

      UNION ALL

      SELECT
      paid_subscribers as user_count
      ,report_date
      ,'ios' as platform
      , billing_period
      FROM active_ios


      ),
      active_count as (
      SELECT
        user_count
        ,report_date
        ,platform
        ,billing_period
      from active_count_pre
      where platform != 'roku'

      UNION ALL

      SELECT
        user_count + 6700 as user_count
        ,report_date
        ,platform
        ,billing_period
      from active_count_pre
      where platform = 'roku' and billing_period = 'monthly'

      UNION ALL

      SELECT
        user_count + 2300 as user_count
        ,report_date
        ,platform
        ,billing_period
      from active_count_pre
      where platform = 'roku' and billing_period = 'yearly'
      ),

      trial_count as (
      SELECT
      count(distinct user_id) as user_count
      ,report_date
      ,platform
      ,billing_period
      FROM trial
      GROUP BY 2,3,4
      ),
      total_trial_count as (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS user_count_7d,
      report_date,
      platform,
      billing_period

      FROM trial_count
      ),

      convert_dunning_count as (
      SELECT
      COUNT(DISTINCT user_id) as user_count
      ,DATE(received_at) as report_date
      ,'web' AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_invoice_dunning_status is not NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2,3,4
      ),
      total_dunning as (
      SELECT

        SUM(user_count) OVER (
        PARTITION BY platform, billing_period
        ORDER BY report_date
          ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS user_count
        ,report_date
        ,platform
        ,billing_period
      FROM convert_dunning_count
      ),
      dunning_paid_count as (
      SELECT
      COUNT(DISTINCT content_subscription_id) as user_count
      ,DATE(received_at) AS report_date
      ,'web' AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= '2025-07-01'
      AND (report_date::date - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
      AND content_invoice_dunning_attempts != '[]'
      GROUP BY 2,3,4
      ),
      total_dunning_paid as (
      SELECT

        SUM(user_count) OVER (
        PARTITION BY platform, billing_period
        ORDER BY report_date
          ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS user_count
        ,report_date
        ,platform
        ,billing_period
      FROM dunning_paid_count
      ),

      dunning_cancelled_count as (
      SELECT
      COUNT(DISTINCT content_customer_id) as user_count
      ,DATE(timestamp) as report_date
      ,'web' AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason is not NULL
      AND content_subscription_cancelled_at -content_customer_created_at < 1900000
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2,3,4
      ),

      result as (
      SELECT
      *
      ,'dunning_gained' as status
      FROM total_dunning

      UNION ALL

      SELECT
      *
      ,'dunning_paid' as status
      FROM total_dunning_paid

      UNION ALL

      SELECT
      *
      ,'dunning_cancelled' as status
      FROM dunning_cancelled_count

      UNION ALL

      SELECT
      *
      ,'active' as status
      FROM active_count

      UNION ALL

      SELECT
      *
      ,'in_trial' as status
      FROM total_trial_count)

      SELECT *,
      'AzZmVjUuQo25N2MFb'::VARCHAR as user_id
      FROM result
      ;;
  }
  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
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

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }


  measure: total_paying {
    type: sum
    filters: [status: "active"]
    sql:${TABLE}.user_count   ;;
  }

  measure: total_free_trials {
    type: sum
    filters: [status: "in_trial"]
    sql: ${TABLE}.user_count ;;
  }

  measure: dunning_sum{
    type: sum
    filters: [status: "dunning_gained"]
    sql: ${TABLE}.user_count ;;
  }

  measure: total_dunning_paid{
    type: sum
    filters: [status: "dunning_paid"]
    sql: ${TABLE}.user_count ;;
  }

  measure: total_dunning_cancelled{
    type: sum
    filters: [status: "dunning_cancelled"]
    sql: ${TABLE}.user_count ;;
  }


}
