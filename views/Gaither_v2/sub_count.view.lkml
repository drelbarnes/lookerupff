view: sub_count {
  derived_table: {
    sql:
       with users as ( SELECT
        *
      FROM
        ${gaither_analytics_v2.SQL_TABLE_NAME}
        ),

      active_users as (
        SELECT
          COUNT(DISTINCT user_id) as user_count
          ,report_date
          ,platform
        FROM users
        WHERE status in ('active','non_renewing','enabled')
        GROUP BY 2,3
        ),

      prior_subs AS (
        SELECT
          report_date
          ,platform
          ,LAG(user_count, 31) OVER (
            PARTITION BY platform
            ORDER BY report_date
          ) AS prior_31_days_subs
        FROM active_users
      )
      SELECT
        user_count
        ,LAG(user_count,31) OVER(PARTITION BY platform ORDER BY report_date) AS prior_31_days_subs
        ,report_date
        ,platform
      FROM active_users

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

  dimension: prior_31_days_subs {
    type: number
    sql: ${TABLE}.prior_31_days_subs ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }
}
