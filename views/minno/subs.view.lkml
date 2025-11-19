view: subs {

  derived_table: {
    sql:
    with chargebee_raw as(
      SELECT
      date(uploaded_at) as report_date
      ,subscription_id as user_id
      ,Case
      WHEN subscription_status = 'non_renewing' THEN 'active'
      ELSE subscription_status
      END AS status

      ,ROW_NUMBER() OVER (PARTITION BY subscription_id, uploaded_at ORDER BY uploaded_at DESC) AS rn
      FROM http_api.chargebee_subscriptions
      WHERE subscription_subscription_items_0_item_price_id LIKE '%Minno%'),
      chargebee_subs as(
      select
      *
      from chargebee_raw
      where rn=1 and status = 'active'
      ),



 trial AS (
  SELECT
      content_subscription_id::varchar                            AS user_id
    , (CASE
         WHEN content_subscription_billing_period_unit = 'month'
           THEN 'monthly'::varchar
         ELSE 'yearly'::varchar
       END)                                                       AS billing_period
    , 'web'::varchar                                             AS platform
    , DATE(DATEADD(hour, -4, received_at))                       AS report_date
  FROM chargebee_webhook_events.subscription_created
  WHERE DATE(DATEADD(hour, -4, received_at)) >= DATE '2025-06-01'
    AND CAST(content_subscription_subscription_items AS varchar) LIKE '%Mi%'
),
trial_count AS (
  SELECT
      COUNT(DISTINCT user_id) AS user_count
    , report_date

  FROM trial
  GROUP BY report_date
),
total_trial_count AS (
  SELECT
      SUM(user_count) OVER (
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS total_free_trials
    , report_date

  FROM trial_count
),
active_count as (
 select
      count(distinct user_id) as user_count
      ,report_date
      FROM chargebee_subs
      GROUP BY 2
      )

SELECT
  a.total_free_trials
  ,b.user_count as total_paid_subs
  ,a.report_date
FROM total_trial_count a
LEFT JOIN active_count b
on a.report_date = b.report_date;;
  }

  dimension: report_date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension: total_paid_subs {
    type: number
    sql: ${TABLE}.total_paid_subs ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

}
