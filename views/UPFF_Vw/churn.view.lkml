view: churn {
  derived_table: {
    sql:
        with cfg AS (
        SELECT report_date
        FROM ${configg.SQL_TABLE_NAME}
        ),
      v2_table AS (
        SELECT *
        FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
        WHERE report_date  >= (SELECT MAX(report_date) FROM cfg)
        ),
      non_web_user AS (
        SELECT
          report_date,
          user_id,
          billing_period,
          platform
        FROM v2_table
        WHERE platform != 'Chargebee'
      ),
      non_web_cancelled AS (
        SELECT
          DATE("timestamp") AS report_date,
          CAST(user_id AS VARCHAR) AS user_id
        FROM vimeo_ott_webhook.customer_product_expired
        WHERE DATE("timestamp") >= (SELECT MAX(report_date) FROM cfg)
      ),
      non_web_cancelled2 AS (
        SELECT
          a.report_date,
          a.user_id,
          b.billing_period,
          b.platform
        FROM non_web_cancelled a
        LEFT JOIN non_web_user b
        ON a.report_date = b.report_date
        AND a.user_id = b.user_id

        UNION ALL

        SELECT
          date(timestamp) as report_date,
          user_id,
          subscription_frequency as billing_period,
          platform
        FROM vimeo_ott_webhook.customer_product_disabled
        where platform != 'api'
      ),
      chargebee_cancelled AS (
        SELECT
        content_subscription_id::VARCHAR AS user_id,
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
        AND content_subscription_subscription_items LIKE '%UP%'
        AND date(timestamp) >= (SELECT MAX(report_date) FROM cfg)
        )

      SELECT
        COUNT(DISTINCT user_id) AS user_count
        ,report_date
        ,billing_period
        ,platform
      FROM non_web_cancelled2
      GROUP BY 2,3,4

      UNION ALL

      SELECT
        COUNT(DISTINCT user_id) AS user_count
        ,report_date
        ,billing_period
        ,platform
      FROM chargebee_cancelled
      GROUP BY 2,3,4



      ;;
  }
  }
