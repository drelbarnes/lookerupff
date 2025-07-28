view: trials_by_platform {
  derived_table: {
    sql:

with chargebee_trial_start as (
      SELECT
      'Chargebee' as platform
      ,date(DATEADD(HOUR, -5, received_at)) as created_at
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_period
      FROM chargebee_webhook_events.subscription_created
      WHERE content_subscription_subscription_items like '%UP%'
),
vimeo_trial_start as(
      SELECT
        platform
        ,user_id
        ,date(DATEADD(HOUR, 0,timestamp)) as created_at
      FROM vimeo_ott_webhook.customer_product_free_trial_created b
      --LEFT JOIN customers.all_customers a
      --ON a.user_id = b.user_id and DATEADD(day, 1, DATE(b.timestamp)) = date(a.report_date)


),
    user_data as (
    SELECT
      platform
      ,created_at
      ,user_id
    FROM chargebee_trial_start

    UNION ALL

    SELECT
      platform
      ,created_at
      ,user_id
    FROM vimeo_trial_start

    ),

    free_trial_created as (
      SELECT
        platform
        ,created_at
        ,COUNT(DISTINCT user_id) AS free_trial_count
      FROM user_data
      GROUP BY platform, created_at
),
    total_free_trials as (
  SELECT
    f.platform,
    f.created_at,
    (
      SELECT SUM(f2.free_trial_count)
      FROM free_trial_created f2
      WHERE f2.platform = f.platform
        AND f2.created_at BETWEEN
          CASE WHEN f.platform = 'roku'
               THEN DATEADD(day, -7, f.created_at)
               ELSE DATEADD(day, -6, f.created_at)
          END
          AND f.created_at
    ) AS total_trials
  FROM free_trial_created f
)

    SELECT
      ft.platform
      ,ft.created_at
      ,ft.free_trial_count
      ,tf.total_trials
    FROM free_trial_created ft
    LEFT JOIN total_free_trials tf
    ON ft.created_at = tf.created_at and ft.platform = tf.platform
      ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.created_at ;;
}
  dimension: platform{
    type:  string
    sql: ${TABLE}.platform ;;
  }
  dimension: free_trial_created {
    type:  number
    sql: ${TABLE}.free_trial_count ;;
  }

  dimension: total_free_trials {
    type:  number
    sql: ${TABLE}.total_trials ;;
  }
  measure: total_trials_created{
    type: sum
    sql: ${TABLE}.free_trial_count ;;

  }
}
