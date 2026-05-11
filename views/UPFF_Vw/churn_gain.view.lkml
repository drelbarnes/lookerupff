# Datagroup definition - place at the model level in your .model.lkml file.
# This should not live inside the view file if your project separates model and view files.
datagroup: churn_gain_datagroup {
  sql_trigger: SELECT
    CASE
      WHEN CAST(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) AS TIME) >= '10:30:00'
      THEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()), 'YYYY-MM-DD')
      ELSE TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '1 day', 'YYYY-MM-DD')
    END ;;
  max_cache_age: "24 hours"
}

view: churn_gain {
  derived_table: {
    sql:
      ,cfg AS (
        SELECT report_date
        FROM ${configg.SQL_TABLE_NAME}
      ),

      v2_table AS (
      SELECT *
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE report_date >= '2026-01-01'
      AND {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      chargebee_cancelled AS (
      SELECT
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (
      content_subscription_cancel_reason_code NOT IN (
      'Not Paid',
      'No Card',
      'Fraud Review Failed',
      'Non Compliant EU Customer',
      'Tax Calculation Failed',
      'Currency incompatible with Gateway',
      'Non Compliant Customer'
      )
      OR content_subscription_cancel_reason_code IS NULL
      )
      AND content_subscription_activated_at IS NOT NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      AND DATE(timestamp) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(timestamp) {% endincrementcondition %}
      ),

      vm_user AS (
      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM v2_table
      WHERE platform != 'Chargebee'
      ),

      vimeo0 AS (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR) AS user_id,
      subscription_frequency::VARCHAR AS billing_period,
      event_type::VARCHAR AS event_type,
      DATE(event_occurred_at) AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      AND DATE(event_occurred_at) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(event_occurred_at) {% endincrementcondition %}
      ),

      vimeo AS (
      SELECT
      b.user_id,
      a.platform,
      b.billing_period,
      b.event_type,
      b.report_date
      FROM vimeo0 b
      LEFT JOIN vm_user a
      ON a.report_date = b.report_date
      AND a.user_id = b.user_id
      ),

      vm AS (
      SELECT
      DATE("timestamp") AS report_date,
      CAST(user_id AS VARCHAR) AS user_id
      FROM vimeo_ott_webhook.customer_product_expired
      WHERE DATE("timestamp") >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE("timestamp") {% endincrementcondition %}
      ),

      vm2 AS (
      SELECT
      a.report_date,
      a.user_id,
      b.billing_period,
      b.platform
      FROM vm a
      LEFT JOIN vm_user b
      ON a.report_date = b.report_date
      AND a.user_id = b.user_id
      ),

      cancelled_user_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM chargebee_cancelled
      GROUP BY 2, 3, 4

      UNION ALL

      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM vm2
      GROUP BY 2, 3, 4
      ),

      re_acquisitions AS (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(received_at) {% endincrementcondition %}

      UNION ALL

      SELECT
      DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(received_at) {% endincrementcondition %}

      UNION ALL

      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM vimeo
      WHERE event_type = 'Direct to Paid'
      ),

      re_acquisition_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM re_acquisitions
      GROUP BY 2, 3, 4
      ),

      trial_conversion AS (
      SELECT
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(received_at) {% endincrementcondition %}

      UNION ALL

      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM vimeo
      WHERE event_type = 'Free Trial to Paid'
      ),

      conversion_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM trial_conversion
      GROUP BY 2, 3, 4
      ),

      dunning AS (
      SELECT
      content_subscription_id::VARCHAR AS user_id,
      'charge_failed'::VARCHAR AS status,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason_code IN (
      'Not Paid',
      'No Card',
      'Fraud Review Failed',
      'Non Compliant EU Customer',
      'Tax Calculation Failed',
      'Currency incompatible with Gateway',
      'Non Compliant Customer'
      )
      AND (content_subscription_cancelled_at - content_subscription_activated_at) > 10000
      AND content_subscription_subscription_items LIKE '%UP%'
      AND DATE(timestamp) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(timestamp) {% endincrementcondition %}
      ),

      dunning_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM dunning
      GROUP BY 2, 3, 4
      ),

      paused AS (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_paused
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)
      AND {% incrementcondition %} DATE(received_at) {% endincrementcondition %}
      ),

      paused_count AS (
      SELECT
      COUNT(DISTINCT user_id) AS user_count,
      report_date,
      billing_period,
      platform
      FROM paused
      GROUP BY 2, 3, 4
      ),

      result AS (
      SELECT
      *,
      'churn'::VARCHAR AS status
      FROM cancelled_user_count

      UNION ALL

      SELECT
      *,
      'paused'::VARCHAR AS status
      FROM paused_count

      UNION ALL

      SELECT
      *,
      'converted'::VARCHAR AS status
      FROM conversion_count

      UNION ALL

      SELECT
      *,
      'reacquisition'::VARCHAR AS status
      FROM re_acquisition_count

      UNION ALL

      SELECT
      *,
      'dunning'::VARCHAR AS status
      FROM dunning_count
      ),

      result2 AS (
      SELECT
      SUM(user_count) AS user_count,
      report_date,
      billing_period,
      status,
      platform
      FROM result
      GROUP BY 2, 3, 4, 5
      ),

      churn_rate AS (
      SELECT
      report_date,
      platform,
      rolling_30_day_unique_user_count_yearly,
      rolling_30_day_unique_user_count_monthly,
      total_rolling_monthly,
      total_rolling_yearly
      FROM ${rolling_platform.SQL_TABLE_NAME}
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      rolling_churn AS (
      SELECT
      report_date,
      platform,
      rolling_30_day_unique_user_count_monthly AS user_count,
      'monthly'::VARCHAR AS billing_period,
      'rolling_churn'::VARCHAR AS status
      FROM churn_rate

      UNION ALL

      SELECT
      report_date,
      platform,
      total_rolling_monthly AS user_count,
      'monthly'::VARCHAR AS billing_period,
      'rolling_total'::VARCHAR AS status
      FROM churn_rate

      UNION ALL

      SELECT
      report_date,
      platform,
      rolling_30_day_unique_user_count_yearly AS user_count,
      'yearly'::VARCHAR AS billing_period,
      'rolling_churn'::VARCHAR AS status
      FROM churn_rate

      UNION ALL

      SELECT
      report_date,
      platform,
      total_rolling_yearly AS user_count,
      'yearly'::VARCHAR AS billing_period,
      'rolling_total'::VARCHAR AS status
      FROM churn_rate
      )

      SELECT * FROM result2

      UNION ALL

      SELECT
      user_count,
      report_date,
      billing_period,
      status,
      platform
      FROM rolling_churn
      ;;

    datagroup_trigger: churn_gain_datagroup
    increment_key: "date"
    increment_offset: 30
    distribution: "report_date"
    sortkeys: ["report_date"]
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: date {
    type: date
    primary_key: yes
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "churn"]
  }

  measure: paused_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "paused"]
  }

  measure: dunning_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "dunning"]
  }

  measure: converted_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "converted"]
  }

  measure: reacquisition_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "reacquisition"]
  }

  measure: rolling_churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_churn"]
  }

  measure: rolling_total_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_total"]
  }
}
