view: sub_count {
  derived_table: {


    sql:
      SELECT
        CAST(user_count      AS BIGINT)  AS user_count
        ,CAST(report_date    AS DATE)    AS report_date
        ,CAST(platform       AS VARCHAR) AS platform
        ,CAST(billing_period AS VARCHAR) AS billing_period
        ,CAST(status         AS VARCHAR) AS status
        ,'AzZmVjUuQo25N2MFb'::VARCHAR    AS user_id
      FROM (

      SELECT user_count, report_date, platform, billing_period, 'dunning_gained'::VARCHAR AS status
      FROM (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      COUNT(DISTINCT user_id) AS user_count
      ,DATE(received_at)      AS report_date
      ,'web'                  AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_invoice_dunning_status IS NOT NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2, 3, 4
      ) convert_dunning_count_pre
      ) total_dunning

      UNION ALL

      SELECT user_count, report_date, platform, billing_period, 'dunning_paid'::VARCHAR AS status
      FROM (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      COUNT(DISTINCT content_subscription_id) AS user_count
      ,DATE(received_at)                      AS report_date
      ,'web'                                  AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= '2025-07-01'
      AND (DATE(received_at) - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
      AND content_invoice_dunning_attempts != '[]'
      GROUP BY 2, 3, 4
      ) dunning_paid_count_pre
      ) total_dunning_paid

      UNION ALL

      SELECT user_count, report_date, platform, billing_period, 'dunning_cancelled'::VARCHAR AS status
      FROM (
      SELECT
      COUNT(DISTINCT content_customer_id) AS user_count
      ,DATE(timestamp)                    AS report_date
      ,'web'                              AS platform
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason IS NOT NULL
      AND content_subscription_cancelled_at - content_customer_created_at < 1900000
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2, 3, 4
      ) dunning_cancelled_count_pre

      UNION ALL

      SELECT user_count, report_date, platform, billing_period, 'active'::VARCHAR AS status
      FROM (
      SELECT user_count, report_date, platform, billing_period
      FROM (
      SELECT user_count, report_date, platform, billing_period
      FROM (
      SELECT
      COUNT(DISTINCT user_id) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      report_date
      ,user_id
      ,CASE
      WHEN platform = 'Chargebee' THEN 'web'
      ELSE platform
      END AS platform
      ,billing_period
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE status IN ('active', 'non_renewing', 'enabled')
      ) active
      WHERE platform NOT IN ('ios')
      GROUP BY 2, 3, 4

      UNION ALL

      SELECT
      paid_subscribers AS user_count
      ,report_date
      ,'ios' AS platform
      ,billing_period
      FROM ${ios.SQL_TABLE_NAME}
      ) active_count_pre
      ) active_pre
      WHERE platform != 'roku'

      UNION ALL

      SELECT
      user_count + 6700 AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT user_count, report_date, platform, billing_period
      FROM (
      SELECT
      COUNT(DISTINCT user_id) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      report_date
      ,user_id
      ,CASE
      WHEN platform = 'Chargebee' THEN 'web'
      ELSE platform
      END AS platform
      ,billing_period
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE status IN ('active', 'non_renewing', 'enabled')
      ) active
      WHERE platform NOT IN ('ios')
      GROUP BY 2, 3, 4

      UNION ALL

      SELECT
      paid_subscribers AS user_count
      ,report_date
      ,'ios' AS platform
      ,billing_period
      FROM ${ios.SQL_TABLE_NAME}
      ) active_count_pre
      ) roku_pre
      WHERE platform = 'roku'
      AND billing_period = 'monthly'

      UNION ALL

      SELECT
      user_count + 2300 AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT user_count, report_date, platform, billing_period
      FROM (
      SELECT
      COUNT(DISTINCT user_id) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      report_date
      ,user_id
      ,CASE
      WHEN platform = 'Chargebee' THEN 'web'
      ELSE platform
      END AS platform
      ,billing_period
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE status IN ('active', 'non_renewing', 'enabled')
      ) active
      WHERE platform NOT IN ('ios')
      GROUP BY 2, 3, 4

      UNION ALL

      SELECT
      paid_subscribers AS user_count
      ,report_date
      ,'ios' AS platform
      ,billing_period
      FROM ${ios.SQL_TABLE_NAME}
      ) active_count_pre
      ) roku_pre2
      WHERE platform = 'roku'
      AND billing_period = 'yearly'
      ) active_count

      UNION ALL

      SELECT user_count_7d AS user_count, report_date, platform, billing_period, 'in_trial'::VARCHAR AS status
      FROM (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS user_count_7d
      ,report_date
      ,platform
      ,billing_period
      FROM (
      SELECT
      COUNT(DISTINCT user_id) AS user_count
      ,report_date
      ,platform
      ,billing_period
      FROM ${free_trials.SQL_TABLE_NAME}
      GROUP BY 2, 3, 4
      ) trial_count
      ) total_trial_count

      ) all_rows

      ;;
    sql_trigger_value:
    SELECT TO_CHAR(
    CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '7 hour',
    'YYYY-MM-DD'
    ) ;;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]
  }


  dimension: date {
    type: date
    datatype: date
    convert_tz: no
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month]
    datatype: date
    convert_tz: no
    sql: ${TABLE}.report_date ;;
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
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: total_paying {
    type: sum
    filters: [status: "active"]
    sql: ${TABLE}.user_count ;;
  }

  measure: total_free_trials {
    type: sum
    filters: [status: "in_trial"]
    sql: ${TABLE}.user_count ;;
  }

  measure: dunning_sum {
    type: sum
    filters: [status: "dunning_gained"]
    sql: ${TABLE}.user_count ;;
  }

  measure: total_dunning_paid {
    type: sum
    filters: [status: "dunning_paid"]
    sql: ${TABLE}.user_count ;;
  }

  measure: total_dunning_cancelled {
    type: sum
    filters: [status: "dunning_cancelled"]
    sql: ${TABLE}.user_count ;;
  }
}
