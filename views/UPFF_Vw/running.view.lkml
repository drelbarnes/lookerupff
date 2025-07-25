view: running {
  derived_table: {
    sql:
    WITH v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
  where report_date >= '2025-01-01'
),

-- Existing CTEs
trial_conversion AS (
  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM v2_table
  WHERE trials_converted = 'Yes'
),

re_acquisitions AS (
  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM v2_table
  WHERE re_acquisition = 'Yes'
),

trial_started AS (
  SELECT
    report_date,
    user_id,
    billing_period,
    DATE_TRUNC('month', report_date) AS month_start
  FROM v2_table
  WHERE DATE(report_date) = DATE(created_at)
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
