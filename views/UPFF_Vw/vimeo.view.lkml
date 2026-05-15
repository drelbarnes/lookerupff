view: vimeo {
  derived_table: {

    datagroup_trigger: vimeo_datagroup
    increment_key: "report_date"
    increment_offset: 7
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      -- FIX: all incrementcondition tags removed from inner CTEs (platform,
      -- customers, chargebee_re_acquisition). A single incrementcondition tag
      -- on the final SELECT is the only injection point.
      WITH platform_pre AS (
        SELECT
          CAST(user_id AS VARCHAR)           AS user_id,
          platform,
          TO_DATE(report_date, 'YYYY-MM-DD') AS report_date
        FROM customers.all_customers
        WHERE action = 'subscription'
          AND platform NOT IN ('api', 'web')
      ),

      platform AS (
      SELECT user_id, platform, report_date
      FROM platform_pre
      ),

      customers_pre AS (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR)              AS user_id,
      subscription_frequency                    AS billing_period,
      event_type,
      DATE(DATEADD(HOUR, -5, event_occurred_at)) AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      AND current_customer_status = 'enabled'
      ),

      customers AS (
      SELECT user_id, billing_period, event_type, report_date
      FROM customers_pre
      ),

      chargebee_re_acquisition_pre AS (
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
      ),

      chargebee_re_acquisition AS (
      SELECT user_id, platform, billing_period, event_type, report_date
      FROM chargebee_re_acquisition_pre
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
      AND b.user_id     = a.user_id
      )

      SELECT user_id, platform, billing_period, event_type, report_date
      FROM (
      SELECT user_id, platform, billing_period, event_type, report_date
      FROM vimeo

      UNION ALL

      SELECT user_id, platform, billing_period, event_type, report_date
      FROM chargebee_re_acquisition
      ) combined
      WHERE (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ;;
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

################################################################################
# Datagroup — triggers the daily incremental run at 11 AM ET
# NOTE: This must be defined at the MODEL level (in your .model.lkml file),
# not inside the view file.
################################################################################
datagroup: vimeo_datagroup {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '7 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
