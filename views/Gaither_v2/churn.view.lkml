view: churn {
  derived_table: {
    sql:  with chargebee_cancelled AS (
        SELECT
        content_subscription_id::VARCHAR AS user_id,
        'cancelled'::VARCHAR AS status,
        CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
        END AS billing_period,
        DATE("timestamp") AS report_date,
        'web'::VARCHAR AS platform
        FROM chargebee_webhook_events.subscription_cancelled
        WHERE
        (
        --(content_subscription_cancel_reason_code not in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer') and
        (content_subscription_cancelled_at - content_subscription_activated_at) > 10000)
        --or content_subscription_cancel_reason_code is null)
        AND content_subscription_subscription_items LIKE '%Ga%'
        --AND date(timestamp) >= (SELECT MAX(report_date) FROM cfg)
        ),
      chargebee_dunning AS (
      SELECT
      content_subscription_id::VARCHAR AS user_id,
      'charge_failed'::VARCHAR AS status,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      DATE("timestamp") AS report_date,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancel_reason_code in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer') and (content_subscription_cancelled_at - content_subscription_activated_at) > 1900800) AND content_subscription_subscription_items LIKE '%Ga%'
      ),
       non_web_cancelled AS (
        SELECT
        CAST(user_id AS VARCHAR) AS user_id,
        'cancelled'::VARCHAR AS status,
        subscription_frequency as billing_period,
        DATE("timestamp") AS report_date,
        platform
        FROM vimeo_ott_webhook_gaithertv.customer_product_expired

      UNION ALL

        SELECT
        CAST(user_id AS VARCHAR) AS user_id,
        'cancelled'::VARCHAR AS status,
        subscription_frequency as billing_period,
        DATE("timestamp") AS report_date,
        platform
        FROM vimeo_ott_webhook_gaithertv.customer_product_expired
        WHERE platform != 'api'
      )
      select * from chargebee_cancelled
      UNION ALL
      SELECT * FROM chargebee_dunning
      UNION ALL
      SELECT * FROM non_web_cancelled

      ;;
  }
}
