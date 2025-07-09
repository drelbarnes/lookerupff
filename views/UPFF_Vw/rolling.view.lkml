view: rolling {
  derived_table: {
    sql:
    with v2_table as(
      SELECT
        *
      FROM
        ${UPFF_analytics_Vw.SQL_TABLE_NAME}
    ),

      user_cancelled_counts AS (
      SELECT
      report_date,
      user_id
      ,billing_period
      FROM
      v2_table
      WHERE
      sub_cancelled = 'Yes'
      AND charge_failed = 'No'
      GROUP BY
      report_date, user_id,billing_period
      ),
      rolling_churn as (
      SELECT
      t1.report_date
      ,COUNT(DISTINCT CASE WHEN t2.billing_period = 'monthly' THEN t2.user_id END) AS rolling_30_day_unique_user_count_monthly
      ,COUNT(DISTINCT CASE WHEN t2.billing_period = 'yearly' THEN t2.user_id END) AS rolling_30_day_unique_user_count_yearly

      FROM
      user_cancelled_counts t1
      JOIN
      user_cancelled_counts t2
      ON t2.report_date BETWEEN t1.report_date - INTERVAL '29 days' AND t1.report_date
      GROUP BY
      t1.report_date
      ORDER BY
      t1.report_date
      ),

    apple AS (
    SELECT
      date("date") AS report_date,
      CASE
        WHEN device = 'apple_tv' THEN 'tvos'
        ELSE 'ios'
      END AS device_group,
      CASE
        WHEN frequency = 'annual' THEN 'yearly'
        ELSE 'monthly'
      END AS billing_period,
      paying_subscribers
  FROM http_api.subscription_summary_daily
),
    new_apple AS (
  SELECT
    report_date,
    SUM(CASE WHEN billing_period = 'monthly' THEN paying_subscribers ELSE 0 END) AS total_paid_subs_monthly,
    SUM(CASE WHEN billing_period = 'yearly' THEN paying_subscribers ELSE 0 END) AS total_paid_subs_yearly
  FROM apple
  WHERE device_group = 'ios'
  GROUP BY report_date
),

      total_paid_subs as (
      SELECT
      report_date,
      COUNT(DISTINCT CASE
    WHEN (status LIKE 'non_renewing' OR status IN ('active', 'enabled'))
         AND billing_period = 'monthly'
    THEN user_id
    ELSE NULL
END) AS total_paid_subs_monthly
,
      COUNT(DISTINCT CASE WHEN ((status) LIKE 'non_renewing' OR status IN ('active', 'enabled'))and billing_period = 'yearly' THEN user_id ELSE NULL END ) AS total_paid_subs_yearly
      FROM v2_table
      where platform != 'ios'
      GROUP BY 1

      ),
      total_paid_subs2 as (
      SELECT
  COALESCE(a.report_date, t.report_date) AS report_date,
  COALESCE(a.total_paid_subs_monthly, 0) + COALESCE(t.total_paid_subs_monthly, 0) AS total_paid_subs_monthly,
  COALESCE(a.total_paid_subs_yearly, 0) + COALESCE(t.total_paid_subs_yearly, 0) AS total_paid_subs_yearly
FROM new_apple a
FULL OUTER JOIN total_paid_subs t
  ON a.report_date = t.report_date),

      result as(
      SELECT
      rc.report_date,
      rc.rolling_30_day_unique_user_count_yearly,
      rc.rolling_30_day_unique_user_count_monthly,
      tps.total_paid_subs_yearly,
      tps.total_paid_subs_monthly,
      LAG(tps.total_paid_subs_monthly, 30) OVER (ORDER BY tps.report_date) as total_rolling_monthly,
      LAG(tps.total_paid_subs_yearly, 30) OVER (ORDER BY tps.report_date) as total_rolling_yearly
      FROM
      rolling_churn rc
      LEFT JOIN
      total_paid_subs2 tps
      ON rc.report_date = tps.report_date)
      select * from result;;

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


  dimension: total_paid_subs_yearly {
    type: number
    sql: ${TABLE}.total_paid_subs_yearly ;;
  }

  dimension: total_paid_subs_monthly {
    type: number
    sql: ${TABLE}.total_paid_subs_monthly ;;
  }


  dimension: yearly_rolling_subs {
    type: number
    sql: ${TABLE}.total_rolling_yearly ;;
    hidden: no
  }
  dimension: monthly_rolling_subs{
    type: number
    sql: ${TABLE}.total_rolling_monthly ;;
    hidden: no
  }

  dimension: 30_day_rolling_churn_monthly {
    type: number
    sql: ${TABLE}.rolling_30_day_unique_user_count_monthly ;;
    hidden: no
  }

  dimension: 30_day_rolling_churn_yearly {
    type: number
    sql: ${TABLE}.rolling_30_day_unique_user_count_yearly ;;
    hidden: no
  }
}
