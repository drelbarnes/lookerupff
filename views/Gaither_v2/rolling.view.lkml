view: rolling {
  derived_table: {
    sql:
    with v2_table as(
      SELECT
        *
      FROM
        ${gaither_analytics_v2.SQL_TABLE_NAME}
    ),

      user_cancelled_counts AS (
      SELECT
      report_date,
      user_id
      FROM
      v2_table
      WHERE
      sub_cancelled = 'Yes'
      AND charge_failed = 'No'
      GROUP BY
      report_date, user_id
      ),
      rolling_churn as (
      SELECT
      t1.report_date,
      COUNT(DISTINCT t2.user_id) AS rolling_30_day_unique_user_count
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

      total_paid_subs as (
      SELECT
      report_date
      ,COUNT(DISTINCT CASE WHEN ((status) LIKE 'non_renewing' OR status IN ('active', 'enabled')) THEN user_id ELSE NULL END) AS total_paid_subs
      FROM v2_table
      GROUP BY 1

      ),
      result as(
      SELECT
      rc.report_date,
      rc.rolling_30_day_unique_user_count,
      tps.total_paid_subs
      ,LAG(tps.total_paid_subs, 30) OVER (ORDER BY tps.report_date) as total_rolling
      FROM
      rolling_churn rc
      LEFT JOIN
      total_paid_subs tps
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

  dimension: total_paid_subs {
    type: number
    sql: ${TABLE}.total_paid_subs ;;
  }

  dimension: rolling_subs {
    type: number
    sql: ${TABLE}.total_rolling ;;
    hidden: no
  }

  dimension: 30_day_rolling_churn {
    type: number
    sql: ${TABLE}.rolling_30_day_unique_user_count ;;
    hidden: no
  }
}
