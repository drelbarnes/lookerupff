view: trial_converted {
  derived_table: {
    sql:
      with users as (
      SELECT
        report_date
        ,user_id
        ,platform
      FROM ${gaither_analytics_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      and trials_converted = 'Yes'

      UNION ALL

      SELECT
        DATE(received_at) AS report_date,
        content_subscription_id::VARCHAR AS user_id,
        /*
        CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
        END AS billing_period,
        */
        'web'::VARCHAR AS platform
        FROM chargebee_webhook_events.subscription_activated
        WHERE content_subscription_subscription_items LIKE '%Gai%'
      --AND DATE(received_at) >= '2025-07-01'
      )

      SELECT
        count(distinct user_id) as user_count
        ,report_date
        ,platform
      FROM users
      GROUP BY 2,3

    ;;
  }
}
