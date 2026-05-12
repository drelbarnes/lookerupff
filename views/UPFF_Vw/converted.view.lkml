################################################################################
# converted.view.lkml
#
# Incremental PDT that tracks free trial conversion outcomes and related
# dunning/cancellation events across web (Chargebee) and Vimeo OTT platforms.
#
# Event categories tracked (via `types` column):
#   - converted          → free trial → paid conversions (no dunning)
#   - dunning_converted  → conversions with outstanding invoices at activation
#   - dunning_paid       → payment succeeded within 14 days, had dunning attempts
#   - dunning_cancelled  → cancellations due to payment failures
#   - not_converted      → cancellations before subscription was ever activated
#   - gained             → historical free trial signups (from free_trials_historical)
#
# Sources:
#   - customers.new_customers → Vimeo OTT trial conversions
#   - chargebee_webhook_events.subscription_activated → web conversions
#   - chargebee_webhook_events.payment_succeeded → dunning recoveries
#   - chargebee_webhook_events.subscription_cancelled → dunning/non-conversion cancels
#   - free_trials_historical → backfilled trial gain events
#
# Output columns: email, billing_period, platform, report_date, types
#
# Incremental config:
#   - Increment key: report_date
#   - Increment offset: 14 days (rebuilds the last 14 days on each run, to capture
#     the full dunning window)
#   - Trigger: converted_datagroup (daily at 11 AM ET)
################################################################################

view: converted {
  derived_table: {

    datagroup_trigger: converted_datagroup
    increment_key: "report_date"
    increment_offset: 14
    distribution_style: even
    sortkeys: ["report_date"]

    sql:
      WITH converted_vimeo_pre AS (
        SELECT
          email,
          subscription_frequency AS billing_period,
          'vimeo' AS platform,
          DATE(event_occurred_at) AS report_date
        FROM customers.new_customers
        WHERE event_type = 'Free Trial to Paid'
      ),

      converted_web_pre AS (
      SELECT
      content_customer_email AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period,
      'web' AS platform,
      DATE(DATEADD(HOUR, 0, received_at)) AS report_date
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND content_subscription_due_invoices_count = 0
      ),

      converted AS (
      SELECT email, billing_period, platform, report_date
      FROM converted_vimeo_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}

      UNION ALL

      SELECT email, billing_period, platform, report_date
      FROM converted_web_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      dunning_converted_pre AS (
      SELECT
      content_customer_email AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period,
      'web' AS platform,
      DATE(DATEADD(HOUR, 0, received_at)) AS report_date
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND content_subscription_due_invoices_count != 0
      ),

      dunning_converted AS (
      SELECT email, billing_period, platform, report_date
      FROM dunning_converted_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      dunning_paid_pre AS (
      SELECT
      content_customer_email::VARCHAR AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform,
      DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second' - INTERVAL '5 hour') + 8 AS report_date
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND (DATE(received_at) - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
      AND content_invoice_dunning_attempts != '[]'
      ),

      dunning_paid AS (
      SELECT email, billing_period, platform, report_date
      FROM dunning_paid_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      dunning_cancelled_pre AS (
      SELECT
      content_customer_email::VARCHAR AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform,
      DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second' - INTERVAL '5 hour') + 8 AS report_date
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
      AND content_subscription_subscription_items LIKE '%UP%'
      ),

      dunning_cancelled AS (
      SELECT email, billing_period, platform, report_date
      FROM dunning_cancelled_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      sub_cancelled_pre AS (
      SELECT
      content_customer_email AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period,
      'web' AS platform,
      DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second' - INTERVAL '5 hour') + 8 AS report_date
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_activated_at IS NULL
      AND content_subscription_subscription_items LIKE '%UP%'
      ),

      sub_cancelled AS (
      SELECT email, billing_period, platform, report_date
      FROM sub_cancelled_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      gained_pre AS (
      SELECT email, billing_period, platform, report_date
      FROM ${free_trials_historical.SQL_TABLE_NAME}
      ),

      gained AS (
      SELECT email, billing_period, platform, report_date
      FROM gained_pre
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      combined AS (
      SELECT email, billing_period, platform, report_date, 'converted' AS types
      FROM converted

      UNION ALL

      SELECT email, billing_period, platform, report_date, 'dunning_converted' AS types
      FROM dunning_converted

      UNION ALL

      SELECT email, billing_period, platform, report_date, 'dunning_paid' AS types
      FROM dunning_paid

      UNION ALL

      SELECT email, billing_period, platform, report_date, 'dunning_cancelled' AS types
      FROM dunning_cancelled

      UNION ALL

      SELECT email, billing_period, platform, report_date, 'gained' AS types
      FROM gained

      UNION ALL

      SELECT email, billing_period, platform, report_date, 'not_converted' AS types
      FROM sub_cancelled
      )

      SELECT
      CAST(email AS VARCHAR) AS email,
      CAST(billing_period AS VARCHAR) AS billing_period,
      CAST(platform AS VARCHAR) AS platform,
      CAST(report_date AS DATE) AS report_date,
      CAST(types AS VARCHAR) AS types
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

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: types {
    type: string
    sql: ${TABLE}.types ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: converted_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "converted"]
  }

  measure: dunninig_converted_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_converted"]
  }

  measure: dunning_paid_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_paid"]
  }

  measure: dunning_cancelled_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_cancelled"]
  }

  measure: not_converted_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "not_converted"]
  }

  measure: trial_7_days_ago {
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "gained"]
  }
}

################################################################################
# Datagroup — triggers the daily incremental run at 11 AM ET
# NOTE: This must be defined at the MODEL level (in your .model.lkml file),
# not inside the view file.
################################################################################
datagroup: converted_datagroup {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '11 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
