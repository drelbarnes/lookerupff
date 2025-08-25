view: churn_gain {
  derived_table: {
    sql:

    WITH v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
  WHERE report_date >= '2025-06-30'
),

chargebee_cancelled AS (
  SELECT
    report_date,
    user_id,
    billing_period,
    'web'::VARCHAR AS platform
  FROM v2_table
  WHERE sub_cancelled = 'Yes' AND platform = 'Chargebee'
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
    AND DATE(event_occurred_at) >= '2025-07-01'
),

vimeo AS (
  SELECT
    b.user_id,
    COALESCE(a.platform, 'ios')::VARCHAR AS platform,   -- avoid NULL platform
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
  WHERE DATE("timestamp") >= '2025-07-01'
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
  GROUP BY 2,3,4

  UNION ALL

  SELECT
    COUNT(DISTINCT user_id) AS user_count,
    report_date,
    billing_period,
    platform
  FROM vm2
  GROUP BY 2,3,4
),

re_acquisitions AS (
  SELECT
    DATE(received_at) AS report_date,
    content_subscription_id::VARCHAR AS user_id,
    CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
    END AS billing_period,
    'web'::VARCHAR AS platform
  FROM chargebee_webhook_events.subscription_reactivated
  WHERE content_subscription_subscription_items LIKE '%UP%'
    AND DATE(received_at) >= '2025-07-01'

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
  GROUP BY 2,3,4
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
    AND DATE(received_at) >= '2025-07-01'

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
  GROUP BY 2,3,4
),

dunning AS (
  SELECT
    content_subscription_id::VARCHAR AS user_id,
    'charge_failed'::VARCHAR AS status,
    CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
    END AS billing_period,
    DATE("timestamp") AS report_date,
    'web'::VARCHAR AS platform
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE content_subscription_cancel_reason = 'not_paid'
    AND content_subscription_subscription_items LIKE '%UP%'
),

dunning_count AS (
  SELECT
    COUNT(DISTINCT user_id) AS user_count,
    report_date,
    billing_period,
    platform
  FROM dunning
  GROUP BY 2,3,4
),

result AS (
  SELECT
    *,
    'churn'::VARCHAR AS status
  FROM cancelled_user_count

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
  GROUP BY 2,3,4,5
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
  platform   -- match column count & type
FROM rolling_churn


;;
  }
  dimension: date {
    type: date
    primary_key: yes
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time

    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status{
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: billing_period{
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "churn"]
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
