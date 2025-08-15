view: rolling {
  derived_table: {
    sql:
    with vimeo_subscriptions as(
      -- select * from customers.all_customers where report_date = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') --
      select * from customers.all_customers where report_date >= '2025-06-30'),
      vimeo_raw0 as (
      select
      CAST(user_id AS VARCHAR(255))
      ,CASE
      WHEN status = 'free_trial' THEN 'in_trial'
      WHEN status = 'expired' THEN 'paused'
      WHEN status = 'cancelled' THEN 'paused'
      WHEN status = 'enabled' THEN 'active'
      ELSE status
      END AS status
      ,platform
      ,CASE
      WHEN frequency = 'custom' THEN 'monthly'
      ELSE frequency
      END as billing_period
      ,customer_created_at
      ,event_created_at
      ,LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) AS prev_status
      ,LAG(platform) OVER (PARTITION BY user_id ORDER BY report_date) AS prev_platform
      ,report_date
      from vimeo_subscriptions
      where action = 'subscription' and platform not in('api','web')
      ),

      vimeo_raw00 as (
      SELECT
      user_id
      ,status
      ,platform
      ,billing_period
      ,customer_created_at
      ,event_created_at
      ,prev_status
      ,prev_platform
      ,CASE
      WHEN status = 'in_trial'
      AND (prev_status is not NULL AND prev_status NOT IN ('free_trial'))
      THEN 'Yes'
      ELSE 'No'
      END AS platform_change
      ,report_date
      from vimeo_raw0
      ),
      vimeo_raw as (
      select
      user_id
      ,status
      ,platform
      ,prev_status
      ,prev_platform
      ,billing_period
      ,platform_change
      ,CASE
      WHEN prev_status = 'paused' THEN date(DATEADD(HOUR, -4, CAST(replace(event_created_at,' UTC','')as DATETIME)))
      WHEN prev_status is NULL THEN DATEADD(DAY, -1, date(report_date))
      ELSE date(DATEADD(HOUR, 0, CAST(replace(customer_created_at,' UTC','')as DATETIME)))
      END AS created_at
      ,CASE
      WHEN prev_status is NULL and status = 'in_trial' THEN 'Yes'
      WHEN platform_change = 'Yes' and report_date != created_at THEN 'Yes'
      WHEN ABS( (report_date::date) - (created_at::date) ) = 1 and EXTRACT(HOUR FROM CAST(replace(customer_created_at,' UTC','')as DATETIME))<4 THEN 'Yes'
      ELSE 'No'
      END AS add_day
      ,date(report_date) as report_date
      from vimeo_raw00
      ),

      vimeo_raw2 as(
      SELECT
      user_id
      ,status
      ,platform
      ,billing_period
      ,created_at
      ,add_day
      ,report_date
      from vimeo_raw

      UNION ALL
      SELECT
      user_id
      ,'in_trial' as status
      ,platform
      ,billing_period
      ,created_at
      ,add_day
      ,date(created_at) as report_date
      from vimeo_raw
      WHERE add_day = 'Yes'
      ),


      result2 as (select
      user_id
      ,status
      ,platform
      ,billing_period
      ,created_at
      ,DATEADD(DAY, 0, report_date) as report_date,
      CASE
      WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='active'
      THEN 'Yes'
      ELSE 'No'
      END AS sub_cancelled

      from vimeo_raw2),
      result3 as(
      select *
      from result2)

      ,final as(
      SELECT
      user_id,
      status,
      platform,
      billing_period,
      date(created_at) as created_at,
      date(report_date) as report_date
      ,sub_cancelled


      -- Fix for trials_converted logic

      FROM result3
      ),
      v2_table as (

      select
      report_date
      ,user_id
      ,status
      ,platform
      ,billing_period
      ,created_at
      ,sub_cancelled
      from final),


      user_cancelled_counts AS (
      SELECT
      report_date,
      user_id
      ,billing_period
      ,DATE_TRUNC('month', report_date) AS month_start

      FROM
      v2_table
      WHERE
      sub_cancelled = 'Yes'

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


      dates AS (
      SELECT DISTINCT
      report_date,
      DATE_TRUNC('month', report_date) AS month_start
      FROM user_cancelled_counts
      ),

      monthly_churn AS (
      SELECT
      d.report_date,
      COUNT(DISTINCT cu.user_id) AS monthly_running_churn
      FROM dates d
      LEFT JOIN user_cancelled_counts cu
      ON cu.billing_period = 'monthly'
      AND cu.month_start = d.month_start
      AND cu.report_date <= d.report_date
      GROUP BY d.report_date
      ),

      yearly_churn AS (
      SELECT
      d.report_date,
      COUNT(DISTINCT cu.user_id) AS yearly_running_churn
      FROM dates d
      LEFT JOIN user_cancelled_counts cu
      ON cu.billing_period = 'yearly'
      AND cu.month_start = d.month_start
      AND cu.report_date <= d.report_date
      GROUP BY d.report_date
      ),

      running_churn as(
      SELECT
      d.report_date,
      COALESCE(m.monthly_running_churn, 0) AS monthly_running_churn,
      COALESCE(y.yearly_running_churn, 0) AS yearly_running_churn
      FROM
      dates d
      LEFT JOIN monthly_churn m ON d.report_date = m.report_date
      LEFT JOIN yearly_churn y ON d.report_date = y.report_date
      ORDER BY d.report_date),

      new_apple AS (
      SELECT *
      FROM ${ios.SQL_TABLE_NAME}
      ),

      new_apple2 as(
      select a.report_date,
      a.paid_subscribers as total_paid_subs_monthly,
      b.paid_subscribers as total_paid_subs_yearly
      from (select * from new_apple where billing_period = 'monthly') a
      LEFT JOIN (select * from new_apple where billing_period = 'yearly') b
      on a.report_date = b.report_date
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
      FROM new_apple2 a
      FULL OUTER JOIN total_paid_subs t
      ON a.report_date = t.report_date),

      chargebee_subscriptions as (
      select * from http_api.chargebee_subscriptions where  date(uploaded_at) >= '2025-07-01' ),

      ------  Chargebee ------
      -- get daily status of each user
      chargebee_raw as(
      SELECT
      date(uploaded_at) as report_date
      ,subscription_id as user_id
      ,Case
      WHEN subscription_status = 'non_renewing' THEN 'active'
      ELSE subscription_status
      END AS status
      ,CASE
      WHEN subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period
      ,ROW_NUMBER() OVER (PARTITION BY subscription_id, uploaded_at ORDER BY uploaded_at DESC) AS rn
      FROM chargebee_subscriptions
      WHERE subscription_subscription_items_0_item_price_id LIKE '%UP%'
      ),
      chargebee_subs as(
      select
      report_date,
      COUNT(DISTINCT CASE
      WHEN status ='active'
      AND billing_period = 'monthly'
      THEN user_id
      ELSE NULL
      END) AS total_paid_subs_monthly
      ,
      COUNT(DISTINCT CASE WHEN status = 'active' and billing_period = 'yearly' THEN user_id ELSE NULL END ) AS total_paid_subs_yearly
      from chargebee_raw
      where rn=1.
      group by 1
      ),

      total_paid_subs3  as (
      select
      a.report_date
      ,a.total_paid_subs_monthly + b.total_paid_subs_monthly total_paid_subs_monthly
      ,a.total_paid_subs_yearly + b.total_paid_subs_yearly AS total_paid_subs_yearly
      FROM total_paid_subs2 a
      LEFT JOIN chargebee_subs b
      ON a.report_date = b.report_date
      ),

      result as(
      SELECT
      rc.report_date,
      rc.rolling_30_day_unique_user_count_yearly,
      rc.rolling_30_day_unique_user_count_monthly,
      tps.total_paid_subs_yearly,
      tps.total_paid_subs_monthly,
      LAG(tps.total_paid_subs_monthly, 30) OVER (ORDER BY tps.report_date) as total_rolling_monthly,
      LAG(tps.total_paid_subs_yearly, 30) OVER (ORDER BY tps.report_date) as total_rolling_yearly,
      yearly_running_churn,
      monthly_running_churn
      FROM
      rolling_churn rc
      LEFT JOIN
      total_paid_subs3 tps
      ON rc.report_date = tps.report_date
      LEFT JOIN running_churn rc2
      ON rc.report_date = rc2.report_date)
      select * from result;;

  }

  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
    primary_key: yes
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;

  }
  dimension: monthly_running_churn {
    type: number
    sql: ${TABLE}.monthly_running_churn ;;
  }

  dimension: yearly_running_churn {
    type: number
    sql: ${TABLE}.yearly_running_churn ;;
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
