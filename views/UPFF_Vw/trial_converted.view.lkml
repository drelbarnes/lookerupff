view: trial_converted {
  derived_table: {



    sql: with v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
  WHERE report_date >= '2025-06-30'
),

/*
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
      (content_subscription_cancelled_at - content_subscription_trial_end) > 10000)
      --or content_subscription_cancel_reason_code is null)
      AND content_subscription_subscription_items LIKE '%UP%'
      ),*/
-- remove comment for dunning cases
      vm_user AS (
      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM v2_table
      WHERE platform != 'Chargebee'
      ),

      vimeo0 AS (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR) AS user_id,
      subscription_frequency::VARCHAR AS billing_period,
      event_type::VARCHAR AS event_type,
      DATE(DATEADD(HOUR, -5, event_occurred_at)) AS report_date
      ,current_customer_status
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      AND DATE(event_occurred_at) >= '2025-07-01'
      ),

      vimeo AS (
      SELECT
      b.user_id,
      a.platform,
      b.billing_period,
      b.event_type,
      b.report_date,
      b.current_customer_status
      FROM vimeo0 b
      LEFT JOIN vm_user a
      ON a.report_date = b.report_date
      AND a.user_id = b.user_id
      ),
      trial_conversion AS (
      SELECT
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= '2025-07-01'
      --AND content_subscription_due_invoices_count = 0

-- remove comment to not include dunning as a conversion
/*
      UNION ALL

      SELECT
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >= '2025-07-01'
      AND (report_date::date - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
      AND content_invoice_dunning_attempts != '[]'
*/

      UNION ALL

      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM vimeo
      WHERE event_type = 'Free Trial to Paid'
      )
      select * from trial_conversion;;
  }
}
