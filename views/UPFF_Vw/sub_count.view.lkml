datagroup: sub_count_datagroup {
  sql_trigger: SELECT
    CASE
      WHEN CAST(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) AS TIME) >= '11:00:00'
      THEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()), 'YYYY-MM-DD')
      ELSE TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '1 day', 'YYYY-MM-DD')
    END ;;
  max_cache_age: "24 hours"
}

view: sub_count_base {
  derived_table: {
    datagroup_trigger: sub_count_datagroup
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      WITH active AS (
        SELECT
          report_date,
          user_id,
          CASE
            WHEN platform = 'Chargebee' THEN 'web'
            ELSE platform
          END AS platform,
          billing_period
        FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
        WHERE status IN ('active','non_renewing','enabled')
      ),

      trial AS (
      SELECT
      report_date,
      user_id,
      platform,
      billing_period
      FROM ${free_trials.SQL_TABLE_NAME}
      ),

      active_ios AS (
      SELECT *
      FROM ${ios.SQL_TABLE_NAME}
      ),

      active_count_pre AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      platform,
      billing_period
      FROM active
      WHERE platform NOT IN ('ios')
      GROUP BY 2,3,4

      UNION ALL

      SELECT
      paid_subscribers AS user_count,
      report_date,
      'ios' AS platform,
      billing_period
      FROM active_ios
      ),

      active_count AS (
      SELECT
      user_count,
      report_date,
      platform,
      billing_period
      FROM active_count_pre
      WHERE platform != 'roku'

      UNION ALL

      SELECT
      user_count + 6700 AS user_count,
      report_date,
      platform,
      billing_period
      FROM active_count_pre
      WHERE platform = 'roku'
      AND billing_period = 'monthly'

      UNION ALL

      SELECT
      user_count + 2300 AS user_count,
      report_date,
      platform,
      billing_period
      FROM active_count_pre
      WHERE platform = 'roku'
      AND billing_period = 'yearly'
      ),

      trial_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      platform,
      billing_period
      FROM trial
      GROUP BY 2,3,4
      ),

      total_trial_count AS (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS user_count,
      report_date,
      platform,
      billing_period
      FROM trial_count
      ),

      convert_dunning_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      DATE(received_at) AS report_date,
      'web' AS platform,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_invoice_dunning_status IS NOT NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2,3,4
      ),

      total_dunning AS (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS user_count,
      report_date,
      platform,
      billing_period
      FROM convert_dunning_count
      ),

      dunning_paid_count AS (
      SELECT
      COUNT(DISTINCT content_subscription_id) AS user_count,
      DATE(received_at) AS report_date,
      'web' AS platform,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= '2025-07-01'
      AND (DATE(received_at) - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
      AND content_invoice_dunning_attempts != '[]'
      GROUP BY 2,3,4
      ),

      total_dunning_paid AS (
      SELECT
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
      ) AS user_count,
      report_date,
      platform,
      billing_period
      FROM dunning_paid_count
      ),

      dunning_cancelled_count AS (
      SELECT
      COUNT(DISTINCT content_customer_id) AS user_count,
      DATE(timestamp) AS report_date,
      'web' AS platform,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason IS NOT NULL
      AND content_subscription_cancelled_at - content_customer_created_at < 1900000
      AND content_subscription_subscription_items LIKE '%UP%'
      GROUP BY 2,3,4
      ),

      result AS (
      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      'dunning_gained' AS status
      FROM total_dunning

      UNION ALL

      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      'dunning_paid' AS status
      FROM total_dunning_paid

      UNION ALL

      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      'dunning_cancelled' AS status
      FROM dunning_cancelled_count

      UNION ALL

      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      'active' AS status
      FROM active_count

      UNION ALL

      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      'in_trial' AS status
      FROM total_trial_count
      )

      SELECT
      user_count,
      report_date,
      platform,
      billing_period,
      status,
      'AzZmVjUuQo25N2MFb'::VARCHAR AS user_id
      FROM result ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
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
}

view: sub_count {
  derived_table: {
    datagroup_trigger: sub_count_datagroup
    increment_key: "report_date"
    increment_offset: 13
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      SELECT
        user_count,
        report_date,
        platform,
        billing_period,
        status,
        user_id
      FROM ${sub_count_base.SQL_TABLE_NAME}
      WHERE {% incrementcondition %} report_date {% endincrementcondition %} ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
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
