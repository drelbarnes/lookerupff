view: churn {
  derived_table: {
    sql:  with churn AS (
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

        UNION ALL

       --non_web_cancelled
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
        FROM vimeo_ott_webhook_gaithertv.customer_product_disabled
        WHERE platform != 'api'
      ),

      churn_count as (
        SELECT
          COUNT(DISTINCT user_id) as user_count
          ,report_date
          ,platform
        FROM churn
        GROUP BY 2,3
      ),

      rolling_churn AS (
        SELECT
          report_date
          ,platform
          ,SUM(user_count) OVER (
            PARTITION BY platform
            ORDER BY report_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) AS rolling_churn_30_days
        FROM churn_count
    )

    SELECT
      a.user_count
      ,b.rolling_churn_30_days
      ,a.report_date
      ,a.platform
    FROM churn_count a
    LEFT JOIN rolling_churn b
    ON a.report_date = b.report_date and a.platform = b.platform



      ;;
  }
}
