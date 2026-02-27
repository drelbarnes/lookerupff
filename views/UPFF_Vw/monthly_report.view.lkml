view: monthly_report {
  derived_table: {
    sql: with trials as (
      select count(user_id) as user_count
      ,report_date
      FROM ${free_trials.SQL_TABLE_NAME}
      GROUP BY 2
    ),

    churn_gain  as (select * from ${churn_gain.SQL_TABLE_NAME}
    ),

    total_sub as (select sum(user_count)as user_count,report_date from ${sub_count.SQL_TABLE_NAME} where status = 'active' group by 2)

    SELECT
      status
      ,user_count
      ,report_date
    FROM churn_gain

    UNION ALL

    SELECT
      'trial_started'::VARCHAR as status
      ,user_count
      ,report_date
    FROM trials

    UNION ALL

    SELECT
    'active'::VARCHAR as status,
    user_count,
    report_date
FROM (
    SELECT
        user_count,
        report_date,
        MAX(report_date) OVER (
            PARTITION BY DATE_TRUNC('month', report_date)
        ) AS month_max_date
    FROM total_sub
) t
WHERE report_date = month_max_date

    ;;
  }

  dimension_group: report_date {
    type: time

    timeframes: [date, week,month]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  measure: month_end_active_count {
    type: max
    filters: [status: "active"]
    sql: ${user_count} ;;
  }

  measure: new_trials {
    type: sum
    filters: [status: "trial_started"]
    sql:   ${TABLE}.user_count;;
  }

  measure: trials_to_paid {
    type: sum
    filters: [status: "converted"]
    sql:   ${TABLE}.user_count;;
  }

  measure: new_paid {
    type: sum
    filters: [status: "reacquisition"]
    sql:   ${TABLE}.user_count;;
  }

  measure: cancelled_paid {
    type: sum
    filters: [status: "churn"]
    sql:   ${TABLE}.user_count;;
  }


}
