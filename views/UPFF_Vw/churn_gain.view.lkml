################################################################################
# churn_gain.view.lkml
#
# Incremental PDT that consolidates daily subscription churn and gain events
# across web (Chargebee) and Vimeo OTT platforms (iOS, tvOS, Android, etc.).
#
# Event categories tracked (via `status` column):
#   - churn          -> cancellations (excluding payment-failure dunning)
#   - paused         -> web subscriptions paused
#   - converted      -> free trial -> paid conversions
#   - reacquisition  -> previously cancelled subscriptions reactivated/resumed
#   - dunning        -> cancellations caused by payment failures
#   - rolling_churn  -> 30-day rolling churn user counts (from rolling_platform)
#   - rolling_total  -> 30-day rolling total user counts (from rolling_platform)
#
# Sources:
#   - chargebee_webhook_events.subscription_cancelled / activated / reactivated /
#     resumed / paused -> web events
#   - customers.new_customers -> Vimeo OTT conversion + reacquisition events
#   - vimeo_ott_webhook.customer_product_expired -> Vimeo OTT churn
#   - UPFF_analytics_Vw_v2 -> platform/billing_period attribution for Vimeo users
#   - rolling_platform -> precomputed 30-day rolling counts
#
# Output columns: user_count, report_date, billing_period, status, platform
#
# Incremental config:
#   - Increment key: report_date
#   - Increment offset: 8 days (rebuilds the last 8 days on each run)
#   - Trigger: churn_gain_datagroup (daily at 10:30 AM ET)
#
# FIX: WITH chain at top level. All row assembly and CAST expressions promoted
# into the all_rows CTE. Terminal SELECT is a clean SELECT * FROM all_rows
# WHERE (incrementcondition) with no alias, no inline logic, and no subquery
# wrapper -- consistent with the pattern used across all incremental PDTs in
# this project. Redundant pass-through CTEs collapsed into their _pre sources.
################################################################################

view: churn_gain {
  derived_table: {

    datagroup_trigger: churn_gain_datagroup
    increment_key: "report_date"
    increment_offset: 8
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      WITH v2_table AS (
        SELECT *
        FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
        WHERE report_date >= '2026-01-01'
      ),

      chargebee_cancelled_pre AS (
      SELECT
      content_subscription_id::VARCHAR            AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                         AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp))         AS report_date,
      'web'::VARCHAR                              AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (
      content_subscription_cancel_reason_code NOT IN (
      'Not Paid', 'No Card', 'Fraud Review Failed',
      'Non Compliant EU Customer', 'Tax Calculation Failed',
      'Currency incompatible with Gateway', 'Non Compliant Customer'
      )
      OR content_subscription_cancel_reason_code IS NULL
      )
      AND content_subscription_activated_at IS NOT NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      ),

      vm_user AS (
      SELECT report_date, user_id, billing_period, platform
      FROM v2_table
      WHERE platform != 'Chargebee'
      ),

      vimeo0_pre AS (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR)     AS user_id,
      subscription_frequency::VARCHAR  AS billing_period,
      event_type::VARCHAR              AS event_type,
      DATE(event_occurred_at)          AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      ),

      vimeo AS (
      SELECT
      b.user_id,
      a.platform,
      b.billing_period,
      b.event_type,
      b.report_date
      FROM vimeo0_pre b
      LEFT JOIN vm_user a
      ON  a.report_date = b.report_date
      AND a.user_id     = b.user_id
      ),

      vm2 AS (
      SELECT
      a.report_date,
      a.user_id,
      b.billing_period,
      b.platform
      FROM (
      SELECT
      DATE("timestamp")        AS report_date,
      CAST(user_id AS VARCHAR) AS user_id
      FROM vimeo_ott_webhook.customer_product_expired
      ) a
      LEFT JOIN vm_user b
      ON  a.report_date = b.report_date
      AND a.user_id     = b.user_id
      ),

      cancelled_user_count AS (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM chargebee_cancelled_pre
      GROUP BY 2, 3, 4

      UNION ALL

      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM vm2
      GROUP BY 2, 3, 4
      ),

      re_acquisitions_pre AS (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items LIKE '%UP%'

      UNION ALL

      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'
      ),

      re_acquisition_count AS (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT report_date, user_id, billing_period, platform FROM re_acquisitions_pre
      UNION ALL
      SELECT report_date, user_id, billing_period, platform FROM vimeo WHERE event_type = 'Direct to Paid'
      ) re_acquisitions
      GROUP BY 2, 3, 4
      ),

      trial_conversion_pre AS (
      SELECT
      DATE(received_at)                        AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      ),

      conversion_count AS (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT report_date, user_id, billing_period, platform FROM trial_conversion_pre
      UNION ALL
      SELECT report_date, user_id, billing_period, platform FROM vimeo WHERE event_type = 'Free Trial to Paid'
      ) trial_conversion
      GROUP BY 2, 3, 4
      ),

      dunning_count AS (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason_code IN (
      'Not Paid', 'No Card', 'Fraud Review Failed',
      'Non Compliant EU Customer', 'Tax Calculation Failed',
      'Currency incompatible with Gateway', 'Non Compliant Customer'
      )
      AND (content_subscription_cancelled_at - content_subscription_activated_at) > 10000
      AND content_subscription_subscription_items LIKE '%UP%'
      ) dunning_pre
      GROUP BY 2, 3, 4
      ),

      paused_count AS (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_paused
      WHERE content_subscription_subscription_items LIKE '%UP%'
      ) paused_pre
      GROUP BY 2, 3, 4
      ),

      result2 AS (
      SELECT
      SUM(user_count) AS user_count,
      report_date,
      billing_period,
      status,
      platform
      FROM (
      SELECT user_count, report_date, billing_period, platform, 'churn'::VARCHAR         AS status FROM cancelled_user_count
      UNION ALL
      SELECT user_count, report_date, billing_period, platform, 'paused'::VARCHAR        AS status FROM paused_count
      UNION ALL
      SELECT user_count, report_date, billing_period, platform, 'converted'::VARCHAR     AS status FROM conversion_count
      UNION ALL
      SELECT user_count, report_date, billing_period, platform, 'reacquisition'::VARCHAR AS status FROM re_acquisition_count
      UNION ALL
      SELECT user_count, report_date, billing_period, platform, 'dunning'::VARCHAR       AS status FROM dunning_count
      ) result
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
      ),

      all_rows AS (
      SELECT
      CAST(user_count     AS BIGINT)  AS user_count,
      CAST(report_date    AS DATE)    AS report_date,
      CAST(billing_period AS VARCHAR) AS billing_period,
      CAST(status         AS VARCHAR) AS status,
      CAST(platform       AS VARCHAR) AS platform
      FROM result2

      UNION ALL

      SELECT
      CAST(rolling_30_day_unique_user_count_monthly AS BIGINT) AS user_count,
      CAST(report_date    AS DATE)                             AS report_date,
      'monthly'::VARCHAR                                       AS billing_period,
      'rolling_churn'::VARCHAR                                 AS status,
      CAST(platform       AS VARCHAR)                          AS platform
      FROM churn_rate

      UNION ALL

      SELECT
      CAST(total_rolling_monthly AS BIGINT) AS user_count,
      CAST(report_date    AS DATE)           AS report_date,
      'monthly'::VARCHAR                     AS billing_period,
      'rolling_total'::VARCHAR               AS status,
      CAST(platform       AS VARCHAR)        AS platform
      FROM churn_rate

      UNION ALL

      SELECT
      CAST(rolling_30_day_unique_user_count_yearly AS BIGINT) AS user_count,
      CAST(report_date    AS DATE)                            AS report_date,
      'yearly'::VARCHAR                                       AS billing_period,
      'rolling_churn'::VARCHAR                                AS status,
      CAST(platform       AS VARCHAR)                         AS platform
      FROM churn_rate

      UNION ALL

      SELECT
      CAST(total_rolling_yearly AS BIGINT) AS user_count,
      CAST(report_date    AS DATE)          AS report_date,
      'yearly'::VARCHAR                     AS billing_period,
      'rolling_total'::VARCHAR              AS status,
      CAST(platform       AS VARCHAR)       AS platform
      FROM churn_rate
      )

      SELECT *
      FROM all_rows
      WHERE (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
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

################################################################################
# Datagroup — triggers the daily incremental run at 10:30 AM ET
# NOTE: This must be defined at the MODEL level (in your .model.lkml file),
# not inside the view file.
################################################################################
datagroup: churn_gain_datagroup {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '6 hour 30 minute',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
