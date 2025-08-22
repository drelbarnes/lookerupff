view: churn_gain {
  derived_table: {
    sql:

    WITH v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
  where report_date >= '2025-06-30' ),


      chargebee_cancelled AS (
      SELECT
        report_date,
        user_id
        ,billing_period
      FROM v2_table
      WHERE sub_cancelled = 'Yes' and platform = 'Chargebee'
      ),

      vm_user  as(
      select
        report_date
        ,user_id
        ,billing_period
        ,platform
      FROM v2_table
      WHERE platform != 'Chargebee'
      ),

      vimeo as (
      select distinct
        CAST(customer_id AS VARCHAR)as user_id
        ,subscription_frequency as billing_period
        ,event_type
        ,date(event_occurred_at) as report_date
      FROM customers.new_customers
      where subscription_frequency != 'custom'
      and date(event_occurred_at) >= '2025-07-01'),


      vm as (
      SELECT
        date(timestamp) as report_date
        ,CAST(user_id AS VARCHAR) as user_id
        FROM vimeo_ott_webhook.customer_product_expired
        where date(timestamp) >='2025-07-01'
      ),
      vm2 as (
      SELECT
        a.report_date
        ,a.user_id
        ,b.billing_period
        from vm a
        LEFT JOIN vm_user b
        ON a.report_date = b.report_date and a.user_id = b.user_id
      ),

      cancelled_user_count as (
        SELECT
          count(distinct user_id) as user_count
          ,report_date
          ,billing_period
        from chargebee_cancelled
        GROUP BY 2,3

        UNION ALL
        select
          count(distinct user_id) as user_count
          ,report_date
          ,billing_period
        from vm2
        GROUP BY 2,3
      ),


    re_acquisitions AS (
  SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items like '%UP%'and date(timestamp) >= '2025-07-01'
  UNION ALL

  SELECT
    report_date,
    user_id,
    billing_period
  FROM vimeo where event_type = 'Direct to Paid'
),
  re_acquisition_count as(
    SELECT
      count(distinct user_id) as user_count
      ,report_date
      ,billing_period
    FROM re_acquisitions
    GROUP BY 2,3
  ),

-- Existing CTEs
trial_conversion AS (
  SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items like '%UP%'and date(timestamp) >= '2025-07-01'
  UNION ALL

  SELECT
    report_date,
    user_id,
    billing_period
  FROM vimeo where event_type = 'Free Trial to Paid'
),

conversion_count as (
  SELECT
    count(distinct user_id) as user_count
    ,report_date
    ,billing_period
  FROM trial_conversion
  GROUP BY 2,3

),

dunning as (
  SELECT
    content_subscription_id as user_id
    ,'charge_failed' as status
    ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
    END AS billing_period
    ,date(timestamp) as report_date
  FROM  chargebee_webhook_events.subscription_cancelled
  WHERE content_subscription_cancel_reason = 'not_paid'
  AND content_subscription_subscription_items like '%UP%'
),

dunning_count as (
  SELECT
    COUNT(DISTINCT user_id) as user_count
    ,report_date
    ,billing_period
  FROM dunning
  GROUP BY 2,3
),
result as (
  SELECT
  *,
  'churn' as status
  FROM cancelled_user_count

  UNION ALL

  SELECT
    *
    ,'converted' as status
  FROM conversion_count

  UNION ALL

  SELECT
    *
    ,'reacquisition' as status
  FROM re_acquisition_count

  UNION ALL
  SELECT
    *
    ,'dunning' as status
  FROM dunning_count
),

result2 as (
  SELECT
    sum(user_count) as user_count
    ,report_date
    ,billing_period
    ,status
  FROM result
  GROUP BY 2,3,4
),
churn_rate as (
SELECT
  report_date
  ,rolling_30_day_unique_user_count_yearly
  ,rolling_30_day_unique_user_count_monthly
  ,total_rolling_monthly
  ,total_rolling_yearly
FROM ${rolling.SQL_TABLE_NAME}),

rolling_churn as(
  SELECT
    report_date
    ,rolling_30_day_unique_user_count_monthly as user_count
    ,'monthly' as billing_period
    ,'rolling_churn' as status
  FROM churn_rate

  UNION ALL

  SELECT
    report_date
    ,total_rolling_monthly as user_count
    ,'monthly' as billing_period
    ,'rolling_total' as status
  FROM churn_rate

  UNION ALL

  SELECT
    report_date
    ,rolling_30_day_unique_user_count_yearly as user_count
    ,'yearly' as billing_period
    ,'rolling_churn' as status
  FROM churn_rate

  UNION ALL

  SELECT
    report_date
    ,total_rolling_yearly as user_count
    ,'yearly' as billing_period
    ,'rolling_total' as status
  FROM churn_rate

)
SELECT * FROM result2
UNION ALL
SELECT
  user_count
  ,report_date
  ,billing_period
  ,status
FROM rolling_churn
;;
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
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status{
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: billing_period{
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "churn"]
  }
  measure: dunning_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "dunning"]

  }
  measure: converted_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "converted"]

  }

  measure: reacquisition_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "reacquisition"]

  }

  measure: rolling_churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_churn"]

  }

  measure: rolling_total_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_total"]

  }

}
