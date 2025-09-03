view: running {
  derived_table: {
    sql:

    with platform as (
      select distinct
        CAST(user_id AS VARCHAR) as user_id
        ,platform
        ,report_date
      from customers.all_customers
      where report_date >= '2025-07-01'
      and action = 'subscription'
    ),

    reacquisition as (
      select distinct
        CAST(customer_id AS VARCHAR)as user_id
        ,subscription_frequency as billing_period
        ,event_type
        ,date(event_occurred_at) as report_date
      FROM customers.new_customers
      where subscription_frequency != 'custom'
      and date(event_occurred_at) >= '2025-07-01'),

    vimeo as (

    SELECT
      b.user_id
      ,a.platform
      ,b.billing_period
      ,b.event_type
      ,b.report_date
    FROM reacquisition b
    LEFT JOIN platform a
    ON a.report_date = b.report_date
    and a.user_id = b.user_id),

-- Existing CTEs
trial_conversion AS (
  SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      ,DATE_TRUNC('month', date(timestamp)) AS month_start
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items like '%UP%'and date(timestamp) >= '2025-07-01'
  UNION ALL

  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM vimeo where event_type = 'Free Trial to Paid'
),

re_acquisitions AS (
  SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      ,DATE_TRUNC('month', date(timestamp)) AS month_start
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items like '%UP%'and date(timestamp) >= '2025-07-01'
  UNION ALL

  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM vimeo where event_type = 'Direct to Paid'
),

trial_started AS (
  SELECT
      --subtract 5 hour delay to get actual time
      date(DATEADD(HOUR, -5, received_at)) as report_date
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      ,DATE_TRUNC('month', date(timestamp))AS month_start
      FROM chargebee_webhook_events.subscription_created
      WHERE content_subscription_subscription_items like '%UP%'and date(timestamp) >= '2025-07-01'
  UNION ALL

  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM vimeo where event_type = 'New Free Trial'
),

-- Base date set from trial_conversion to unify report_date dimension
dates AS (
  SELECT DISTINCT
    report_date,
    DATE_TRUNC('month', report_date) AS month_start
  FROM trial_conversion
),

-- Trial conversion running counts
monthly_conversion AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT cu.user_id) AS monthly_running_conversion
  FROM dates d
  LEFT JOIN trial_conversion cu
    ON cu.billing_period = 'monthly'
    AND cu.month_start = d.month_start
    AND cu.report_date <= d.report_date
  GROUP BY d.report_date
),

yearly_conversion AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT cu.user_id) AS yearly_running_conversion
  FROM dates d
  LEFT JOIN trial_conversion cu
    ON cu.billing_period = 'yearly'
    AND cu.month_start = d.month_start
    AND cu.report_date <= d.report_date
  GROUP BY d.report_date
),

-- Re-acquisition running counts
monthly_reacq AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT ra.user_id) AS monthly_running_reacquisition
  FROM dates d
  LEFT JOIN re_acquisitions ra
    ON ra.billing_period = 'monthly'
    AND ra.month_start = d.month_start
    AND ra.report_date <= d.report_date
  GROUP BY d.report_date
),

yearly_reacq AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT ra.user_id) AS yearly_running_reacquisition
  FROM dates d
  LEFT JOIN re_acquisitions ra
    ON ra.billing_period = 'yearly'
    AND ra.month_start = d.month_start
    AND ra.report_date <= d.report_date
  GROUP BY d.report_date
),

-- Trial started running counts
monthly_trials_started AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT ts.user_id) AS monthly_running_trials_started
  FROM dates d
  LEFT JOIN trial_started ts
    ON ts.billing_period = 'monthly'
    AND ts.month_start = d.month_start
    AND ts.report_date <= d.report_date
  GROUP BY d.report_date
),

yearly_trials_started AS (
  SELECT
    d.report_date,
    COUNT(DISTINCT ts.user_id) AS yearly_running_trials_started
  FROM dates d
  LEFT JOIN trial_started ts
    ON ts.billing_period = 'yearly'
    AND ts.month_start = d.month_start
    AND ts.report_date <= d.report_date
  GROUP BY d.report_date
),

-- Final result
result AS (
  SELECT
    d.report_date,
    mc.monthly_running_conversion,
    yc.yearly_running_conversion,
    mr.monthly_running_reacquisition,
    yr.yearly_running_reacquisition,
    mts.monthly_running_trials_started,
    yts.yearly_running_trials_started
  FROM dates d
  LEFT JOIN monthly_conversion mc ON d.report_date = mc.report_date
  LEFT JOIN yearly_conversion yc ON d.report_date = yc.report_date
  LEFT JOIN monthly_reacq mr ON d.report_date = mr.report_date
  LEFT JOIN yearly_reacq yr ON d.report_date = yr.report_date
  LEFT JOIN monthly_trials_started mts ON d.report_date = mts.report_date
  LEFT JOIN yearly_trials_started yts ON d.report_date = yts.report_date
)

SELECT * FROM result
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
  }
  dimension: monthly_running_conversion {
    type:  number
    sql: ${TABLE}.monthly_running_conversion ;;
    label: "Running Conversion Monthly"
  }

  dimension: yearly_running_conversion {
    type:  number
    sql: ${TABLE}.yearly_running_conversion ;;
    label: "Running Conversion Yearly"
  }
  dimension: monthly_running_reacquisition {
    type: number
    sql: ${TABLE}.monthly_running_reacquisition ;;
    label: "Running Reacquisition Monthly"
  }

  dimension: yearly_running_reacquisition {
    type: number
    sql: ${TABLE}.yearly_running_reacquisition ;;
    label: "Running Reacquisition Yearly"
  }

  dimension: monthly_running_trials_started {
    type: number
    sql: ${TABLE}.monthly_running_trials_started ;;
    label: "Running Trials Started Monthly"
  }

  dimension: yearly_running_trials_started {
    type: number
    sql: ${TABLE}.yearly_running_trials_started ;;
    label: "Running Trials Started Yearly"


  }
  }
