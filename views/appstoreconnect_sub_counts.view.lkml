view: appstoreconnect_sub_counts {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: with reports as (
      select
      id
      , "date" as report_date
      , device
      , frequency
      , free_trials
      , paying_subscribers
      , billing_retries
      from http_api.subscription_summary_daily
      order by date desc
    )
    , report_dates as (
      with generate_numbers as (
        SELECT ROW_NUMBER() OVER (order by report_date) as n
        FROM customers.all_customers
        LIMIT 5000
      )
      , generate_dates AS (
        SELECT
        (CURRENT_DATE - n)::date as report_date
        FROM generate_numbers
        WHERE n <= 5000
      )
      select
      report_date
      from generate_dates
      where report_date >= '2022-04-06'
    )
    , agg as (
      select
      a.report_date
      , sum(case when b.device in ('iphone', 'ipad', 'ipod_touch') then b.paying_subscribers else null end) as ios
      , sum(case when b.device = 'apple_tv' then b.paying_subscribers else null end) as tvos
      from report_dates a
      left join reports b
      on a.report_date = b.report_date
      group by a.report_date
    )
    , outer_query as (
    SELECT
      report_date,
      COALESCE(NULLIF(ios, 0), last_ios) AS ios,
      COALESCE(NULLIF(tvos, 0), last_tvos) AS tvos
    FROM (
      SELECT
        report_date,
        ios,
        tvos,
        LAST_VALUE(NULLIF(ios, 0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_ios,
        LAST_VALUE(NULLIF(tvos, 0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_tvos
      FROM agg
    ) sub
    ORDER BY report_date DESC
    )
    select * from outer_query order by report_date
      ;;
  }

  dimension: ios {
    description: "Number of iOS paying subscribers"
    type: number
    sql: ${TABLE}.ios ;;
  }

  dimension: tvos {
    description: "Number of tvOS paying subscribers"
    type: number
    sql: ${TABLE}.tvos ;;
  }

  dimension_group: report_date {
    type: time
    sql: ${TABLE}.report_date ;;
  }

}
