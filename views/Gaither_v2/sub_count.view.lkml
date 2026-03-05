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
        a.user_count
        ,b.prior_31_days_subs
        ,a.report_date
        ,a.platform
      FROM active_users a
      LEFT JOIN prior_subs b
      ON a.report_date = b.report_date and a.platform = b.platform

    ;;
  }
}
