view: ltv {
  derived_table: {
    sql: WITH v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
  where report_date >= '2025-06-30' ),

  vm as (
      SELECT
        date(timestamp) as report_date
        ,CAST(user_id AS VARCHAR) as user_id
        ,DATE_TRUNC('month', timestamp) AS month_start
        ,platform
        FROM vimeo_ott_webhook.customer_product_expired
        where date(timestamp) >='2025-07-01'
        and platform !='api'

        UNION ALL

        SELECT
        date(timestamp) as report_date
        ,CAST(user_id AS VARCHAR) as user_id
        ,DATE_TRUNC('month', timestamp) AS month_start
        ,platform
        FROM vimeo_ott_webhook.customer_product_disabled
        where date(timestamp) >='2025-07-01'
        and platform !='api'
      ),

rolling_churn AS (
  SELECT
    t1.report_date,
    COUNT(DISTINCT t2.user_id) AS churn_30_days
  FROM vm t1
  JOIN vm t2
    ON t2.report_date BETWEEN t1.report_date - INTERVAL '29 days'
                          AND t1.report_date
  GROUP BY t1.report_date
  ORDER BY t1.report_date
),
subs as (
SELECT
    report_date,
    sum(user_count) as user_count
  FROM ${sub_count.SQL_TABLE_NAME}
  where platform != 'web'
  group by 1
),
prior_subs AS (
  SELECT
    report_date,
    LAG(user_count, 31) OVER (ORDER BY report_date) AS prior_31_days_subs
  FROM subs
),



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
FROM result
;;

  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;

  }

  dimension: LTV {
    type: number
    sql: ${TABLE}.LTV ;;

  }
  dimension: prior_31_days_subs {
    type: number
    sql: ${TABLE}.prior_31_days_subs ;;

  }
  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;

  }
  }
