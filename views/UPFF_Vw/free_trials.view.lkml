################################################################################
# free_trials.view.lkml
#
# Incremental PDT that tracks new free trial signups across web (Chargebee)
# and Vimeo OTT platforms (iOS, tvOS, Android, Android TV, Amazon Fire TV/Tablet,
# Roku, Vizio).
#
# Sources:
#   - chargebee_webhook_events.subscription_created → web free trials
#   - customers.new_customers (event_type = 'New Free Trial') → Vimeo OTT trials
#   - vimeo_ott_webhook.customer_product_free_trial_created → platform attribution
#
# Output columns: user_id, billing_period, platform, report_date
#
# Incremental config:
#   - Increment key: report_date
#   - Increment offset: 7 days (rebuilds the last 7 days on each run)
#   - Trigger: free_trials_datagroup (daily at 10 AM ET)
#
# Measures: distinct free trial counts, total and broken out by platform.
################################################################################

view: free_trials {
  derived_table: {

    datagroup_trigger: free_trials_datagroup
    increment_key: "report_date"
    increment_offset: 7
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      WITH chargebee_pre AS (
        SELECT
          content_subscription_id AS user_id,
          CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
          END AS billing_period,
          'web' AS platform,
          DATE(DATEADD(HOUR, -4, received_at)) AS report_date
        FROM chargebee_webhook_events.subscription_created
        WHERE content_subscription_subscription_items LIKE '%UP%'
      ),

      chargebee AS (
      SELECT user_id, billing_period, platform, report_date
      FROM chargebee_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      vimeo_pre AS (
      SELECT DISTINCT
      email,
      DATE(event_occurred_at) AS report_date,
      subscription_frequency AS billing_period
      FROM customers.new_customers
      WHERE event_type = 'New Free Trial'
      ),

      vimeo AS (
      SELECT email, report_date, billing_period
      FROM vimeo_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      vimeo_platform_pre AS (
      SELECT
      email,
      platform,
      DATE(timestamp) AS report_date
      FROM vimeo_ott_webhook.customer_product_free_trial_created
      WHERE DATE(timestamp) >= '2025-06-01'
      ),

      vimeo_platform AS (
      SELECT email, platform, report_date
      FROM vimeo_platform_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      vimeo2 AS (
      SELECT
      a.email AS user_id,
      a.billing_period,
      b.platform,
      a.report_date
      FROM vimeo a
      LEFT JOIN vimeo_platform b
      ON a.email = b.email
      AND a.report_date = b.report_date
      ),

      combined AS (
      SELECT user_id, billing_period, platform, report_date
      FROM vimeo2

      UNION ALL

      SELECT user_id, billing_period, platform, report_date
      FROM chargebee
      )

      SELECT
      CAST(user_id AS VARCHAR) AS user_id,
      CAST(billing_period AS VARCHAR) AS billing_period,
      CAST(platform AS VARCHAR) AS platform,
      CAST(report_date AS DATE) AS report_date
      FROM combined
      ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: free_trials {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
  }

  measure: free_trials_ios {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "ios"]
  }

  measure: free_trials_tvos {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "tvos"]
  }

  measure: free_trials_web {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "web"]
  }

  measure: free_trials_android {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "android"]
  }

  measure: free_trials_android_tv {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "android_tv"]
  }

  measure: free_trials_fire_tv {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "amazon_fire_tv"]
  }

  measure: free_trials_fire_tablet {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "amazon_fire_tablet"]
  }

  measure: free_trials_roku {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "roku"]
  }

  measure: free_trials_vizio {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: [platform: "vizio_tv"]
  }
}

################################################################################
# Datagroup — triggers the daily incremental run at 10 AM ET
# NOTE: This must be defined at the MODEL level (in your .model.lkml file),
# not inside the view file.
################################################################################
datagroup: free_trials_datagroup {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '6 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
