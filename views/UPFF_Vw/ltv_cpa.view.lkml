view: ltv_cpa {
  derived_table: {
    sql: WITH v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
  where report_date >= '2025-12-30'
  ),
  cancelled_user as (
    SELECT * FROM ${churn.SQL_TABLE_NAME}
  ),
spend as(
  SELECT
    date_start as report_date
    ,sum(spend) as spend
  FROM ${daily_spend.SQL_TABLE_NAME}
  group by 1
),
rolling_spend as (
SELECT
    report_date,
    SUM(spend)
      OVER (
        ORDER BY report_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS rolling_spend
FROM spend
),

trial_converted as (
select * from ${trial_converted.SQL_TABLE_NAME}
),
daily_converted_counts AS (
  SELECT
      report_date,
      COUNT(user_id) AS user_count
  FROM trial_converted
  GROUP BY report_date
),
rolling_converted as (
SELECT
    report_date,
    SUM(user_count)
      OVER (
        ORDER BY report_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS rolling_converted
FROM daily_converted_counts
),
rolling_churn AS (
  SELECT
    report_date
    ,platform
    ,billing_period
    ,SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_churn_30_days
  FROM cancelled_user
),
subs as (
SELECT
    report_date
    ,platform
    ,billing_period
    ,user_count
  FROM ${sub_count.SQL_TABLE_NAME}
  where status = 'active'
),
prior_subs AS (
  SELECT
    report_date
    ,platform
    ,billing_period
    ,LAG(user_count, 31) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
    ) AS prior_31_days_subs
  FROM subs
),

result as (
  SELECT
    a.rolling_churn_30_days
    ,b.prior_31_days_subs
    ,a.platform
    ,a.billing_period
    ,a.report_date
  FROM rolling_churn a
  LEFT JOIN prior_subs b
  ON a.report_date = b.report_date and a.platform = b.platform and a.billing_period = b.billing_period
),

cpa as (
select
a.report_date
,a.rolling_spend
,b.rolling_converted
FROM rolling_spend a
LEFT JOIN rolling_converted b
ON a.report_date = b.report_date
)

select
  a.rolling_churn_30_days
  ,a.prior_31_days_subs
  ,a.platform
  ,a.billing_period
  ,a.report_date
  ,b.rolling_spend
  ,b.rolling_converted
FROM result a
LEFT JOIN cpa b
ON a.report_date = b.report_date
/*
result AS (
  SELECT
    rc.report_date,
    ps.prior_31_days_subs,
    rc.churn_30_days,
    CASE
      WHEN rc.report_date > DATE '2020-08-18'
        THEN 4.1
        ELSE 3.69
    END
    /
    NULLIF(
      rc.churn_30_days::DECIMAL
      /
      NULLIF(ps.prior_31_days_subs::DECIMAL, 0),
      0
    ) AS LTV
  FROM rolling_churn rc
  JOIN prior_subs ps
    ON rc.report_date = ps.report_date
)
SELECT
  report_date,
  LTV,
  churn_30_days,
  prior_31_days_subs
FROM result */
;;

  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;

  }

  measure: rolling_converted {
    type: max
    sql: ${TABLE}.rolling_converted ;;

  }

  measure: rolling_spend {
    type: max
    sql: ${TABLE}.rolling_spend ;;

  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;

  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;

  }

  measure: prior_31_days_subs_count {
    type: sum
    sql: ${TABLE}.prior_31_days_subs;;
  }
  measure: rolling_churn_30_days_count {
    type: sum
    sql: ${TABLE}.rolling_churn_30_days;;
  }


  }
