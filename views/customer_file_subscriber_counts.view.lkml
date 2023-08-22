view: customer_file_subscriber_counts {
  derived_table: {
    sql:
    with monthly_customer_report as
    (
      select
      distinct user_id
      , email
      , first_name
      , last_name
      , city
      , state
      , country
      , product_id
      , product_name
      , action
      , action_type
      , status
      , frequency
      , platform
      , coupon_code
      , coupon_code_id
      , promotion_id
      , promotion_id_long
      , promotion_code
      , campaign
      , referrer
      , event_created_at
      , customer_created_at
      , expiration_date
      , marketing_opt_in
      , report_date
      from customers.all_customers
      where action != 'follow'
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
    , count(case when b.status = 'enabled' and b.platform = 'ios' then b.user_id else null end) as ios
    , count(case when b.status = 'enabled' and b.platform = 'tvos' then b.user_id else null end) as tvos
    , count(case when b.status = 'enabled' and b.platform = 'roku' then b.user_id else null end) as roku
    , count(case when b.status = 'enabled' and b.platform = 'amazon_fire_tv' then b.user_id else null end) as amazon_fire_tv
    , count(case when b.status = 'enabled' and b.platform = 'android' then b.user_id else null end) as android
    , count(case when b.status = 'enabled' and b.platform = 'android_tv' then b.user_id else null end) as android_tv
    , count(case when b.status = 'enabled' and b.platform = 'web' then b.user_id else null end) as web
    , count(case when b.status = 'enabled' and b.platform = 'api' then b.user_id else null end) as api
    from report_dates a
    left join monthly_customer_report b
    on a.report_date = b.report_date
    group by 1
    order by 1 desc
    )
    SELECT
      report_date,
      COALESCE(nullif(ios,0), last_ios) AS ios,
      COALESCE(nullif(tvos,0), last_tvos) AS tvos,
      COALESCE(nullif(roku,0), last_roku) AS roku,
      COALESCE(nullif(amazon_fire_tv,0), last_amazon_fire_tv) AS amazon_fire_tv,
      COALESCE(nullif(android,0), last_android) AS android,
      COALESCE(nullif(android_tv,0), last_android_tv) AS android_tv,
      COALESCE(nullif(web,0), last_web) AS web,
      COALESCE(nullif(api,0), last_api) AS api
    FROM (
      SELECT
        report_date,
        ios,
        tvos,
        roku,
        amazon_fire_tv,
        android,
        android_tv,
        web,
        api,
        LAST_VALUE(nullif(ios,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_ios,
        LAST_VALUE(nullif(tvos,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_tvos,
        LAST_VALUE(nullif(roku,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_roku,
        LAST_VALUE(nullif(amazon_fire_tv,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_amazon_fire_tv,
        LAST_VALUE(nullif(android,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_android,
        LAST_VALUE(nullif(android_tv,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_android_tv,
        LAST_VALUE(nullif(web,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_web,
        LAST_VALUE(nullif(api,0)) IGNORE NULLS OVER (ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_api
      FROM agg
    ) sub
    ORDER BY report_date
    ;;
    datagroup_trigger: upff_customer_file_reporting
    distribution_style: all
  }

  dimension: report_date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension: ios {
    type: number
    sql: ${TABLE}.ios ;;
  }

  dimension: tvos {
    type: number
    sql: ${TABLE}.tvos ;;
  }

  dimension: roku {
    type: number
    sql: ${TABLE}.roku ;;
  }

  dimension: amazon_fire_tv {
    type: number
    sql: ${TABLE}.amazon_fire_tv ;;
  }

  dimension: android {
    type: number
    sql: ${TABLE}.android ;;
  }

  dimension: android_tv {
    type: number
    sql: ${TABLE}.android_tv ;;
  }

  dimension: web {
    type: number
    sql: ${TABLE}.web ;;
  }

  dimension: api {
    type: number
    sql: ${TABLE}.api ;;
  }
}
