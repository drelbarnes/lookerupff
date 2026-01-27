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

converted_count as (
      SELECT
      COUNT(DISTINCT user_id) as user_count
      ,DATE(received_at) as report_date
      /*
      ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period
      */
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%Mi%'
      GROUP BY 2
      ),

      re_acquisition as(
      SELECT
      content_subscription_id as user_id
      ,date(DATEADD(HOUR, -5, timestamp)) as report_date

      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items like '%Mi%'

      UNION ALL

      SELECT
        content_subscription_id AS user_id
        ,date(DATEADD(HOUR, -5, timestamp)) AS report_date
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%Mi%' ),

      re_acquisition_count as (
      SELECT
        COUNT(user_id) as user_count
        ,report_date
      FROM re_acquisition
      GROUP BY 2
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
  ,c.user_count as trials_created
  ,d.user_count as converted_count
  ,e.user_count as re_acquisition_count
  ,a.report_date
FROM total_trial_count a
LEFT JOIN active_count b
on a.report_date = b.report_date
LEFT JOIN trial_count c
ON a.report_date = c.report_date
LEFT JOIN converted_count d
ON a.report_date = d.report_date
LEFT JOIN re_acquisition_count e
ON a.report_date = e.report_date;;
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

  dimension: trials_created {
    type: number
    sql: ${TABLE}.trials_created ;;
  }

  measure: converted_count {
    type: sum
    sql: ${TABLE}.converted_count ;;
  }

  measure: re_acquisition_count {
    type: sum
    sql: ${TABLE}.re_acquisition_count ;;
  }

}
