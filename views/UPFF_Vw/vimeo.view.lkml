# Datagroup definition - place at the model level in your .model.lkml file.
# This should not live inside the view file if your project separates model and view files.
datagroup: vimeo_datagroup {
  sql_trigger: SELECT
    CASE
      WHEN CAST(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) AS TIME) >= '10:00:00'
      THEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()), 'YYYY-MM-DD')
      ELSE TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '1 day', 'YYYY-MM-DD')
    END ;;
  max_cache_age: "24 hours"
}

view: vimeo {
  derived_table: {
    sql:
      ,cfg AS (
        SELECT MAX(report_date) AS report_date
        FROM ${configg.SQL_TABLE_NAME}
      ),

      platform AS (
      SELECT
      CAST(user_id AS VARCHAR) AS user_id,
      platform,
      TO_DATE(report_date, 'YYYY-MM-DD') AS report_date
      FROM customers.all_customers
      WHERE TO_DATE(report_date, 'YYYY-MM-DD') >= (
      SELECT MAX(report_date)
      FROM cfg
      )
      AND action = 'subscription'
      AND platform NOT IN ('api', 'web')
      AND {% incrementcondition %} TO_DATE(report_date, 'YYYY-MM-DD') {% endincrementcondition %}
      ),

      customers AS (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR) AS user_id,
      subscription_frequency AS billing_period,
      event_type,
      DATE(DATEADD(HOUR, -5, event_occurred_at)) AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      AND DATE(event_occurred_at) >= (
      SELECT MAX(report_date)
      FROM cfg
      )
      AND current_customer_status = 'enabled'
      AND {% incrementcondition %} DATE(DATEADD(HOUR, -5, event_occurred_at)) {% endincrementcondition %}
      ),

      chargebee_re_acquisition AS (
      SELECT
      content_subscription_id AS user_id,
      'web' AS platform,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period,
      'Direct to Paid' AS event_type,
      DATE(DATEADD(HOUR, -5, timestamp)) AS report_date
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(DATEADD(HOUR, -5, timestamp)) >= (
      SELECT MAX(report_date)
      FROM cfg
      )
      AND {% incrementcondition %} DATE(DATEADD(HOUR, -5, timestamp)) {% endincrementcondition %}

      UNION ALL

      SELECT
      content_subscription_id AS user_id,
      'web' AS platform,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period,
      'Direct to Paid' AS event_type,
      DATE(DATEADD(HOUR, -5, timestamp)) AS report_date
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(DATEADD(HOUR, -5, timestamp)) >= (
      SELECT MAX(report_date)
      FROM cfg
      )
      AND {% incrementcondition %} DATE(DATEADD(HOUR, -5, timestamp)) {% endincrementcondition %}
      ),

      vimeo AS (
      SELECT
      b.user_id,
      a.platform,
      b.billing_period,
      b.event_type,
      b.report_date
      FROM customers b
      LEFT JOIN platform a
      ON a.report_date = b.report_date
      AND b.user_id = a.user_id
      )

      SELECT * FROM vimeo

      UNION ALL

      SELECT * FROM chargebee_re_acquisition
      ;;

    datagroup_trigger: vimeo_datagroup
    increment_key: "report_date"
    increment_offset: 7
    distribution: "report_date"
    sortkeys: ["report_date"]
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: free_trials_gained {
    type: count_distinct
    filters: [event_type: "New Free Trial"]
    sql: ${TABLE}.user_id ;;
  }

  measure: trials_converted {
    type: count_distinct
    filters: [event_type: "Free Trial to Paid"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_ios {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "ios"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_tvos {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "tvos"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_android {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "android"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_android_tv {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "android_tv"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_roku {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "roku"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_amazon_fire_tv {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "amazon_fire_tv"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_amazon_fire_tablet {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "amazon_fire_tablet"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_web {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "web"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed_vizio {
    type: count_distinct
    filters: [event_type: "Direct to Paid", platform: "vizio_tv"]
    sql: ${TABLE}.user_id ;;
  }

  measure: resubscribed {
    type: count_distinct
    filters: [event_type: "Direct to Paid"]
    sql: ${TABLE}.user_id ;;
  }
}
