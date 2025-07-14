view: ios {
  derived_table: {
    sql: with apple AS (
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
 generate_numbers as (
        SELECT ROW_NUMBER() OVER (order by date(analytics_timestamp)) as n
        FROM php.get_analytics
        LIMIT 5000
      )
      , generate_dates AS (
        SELECT
        (CURRENT_DATE - n)::date as report_date
        FROM generate_numbers
        WHERE n <= 5000
      ),
      report_dates as(
      select
      report_date
      from generate_dates
      where report_date >= '2022-06-18'),
  new_apple2 as(
      SELECT
        rd.report_date
        ,a.total_paid_subs_monthly
        ,total_paid_subs_yearly
      FROM report_dates rd
      LEFT JOIN new_apple a
      on rd.report_date = a.report_date
  ),
    result as (
    SELECT
      report_date
      ,COALESCE(NULLIF(total_paid_subs_monthly,0),last_monthly) as total_paid_subs_monthly
      ,COALESCE(NULLIF(total_paid_subs_yearly,0),last_yearly) as total_paid_subs_yearly
    FROM (
    SELECT
      report_date
      ,total_paid_subs_monthly
      ,total_paid_subs_yearly
      ,LAST_VALUE(NULLIF(total_paid_subs_monthly, 0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_monthly
      ,LAST_VALUE(NULLIF(total_paid_subs_yearly, 0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_yearly
      FROM new_apple2
    ) sub
  )
select * from result ;;
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
  type:  number
  sql: ${TABLE}.total_paid_subs_yearly ;;
}

  dimension: total_paid_subs_monthly {
    type:  number
    sql: ${TABLE}.total_paid_subs_monthly ;;
  }


}
