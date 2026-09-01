view: churn_gateway {derived_table: {
  sql:


      WITH cancelled AS (
    SELECT
        content_customer_id::VARCHAR AS user_id,
        CASE
            WHEN content_subscription_billing_period_unit = 'month'
                THEN 'monthly'::VARCHAR
            ELSE 'yearly'::VARCHAR
        END AS billing_period,
        DATE(DATEADD(HOUR, +18, timestamp)) AS report_date,
        'web'::VARCHAR AS platform,
        content_card_gateway AS gateway,
        content_subscription_cancel_reason_code AS cancel_reason,
        CASE
            WHEN content_subscription_cancelled_at - content_subscription_created_at<2000800 THEN 'trial'
            ELSE 'renew'
        END AS trialist
    FROM chargebee_webhook_events.subscription_cancelled c
    WHERE content_subscription_cancel_reason_code IN (
        'Not Paid',
        'No Card',
        'Fraud Review Failed',
        'Non Compliant EU Customer',
        'Tax Calculation Failed',
        'Currency incompatible with Gateway',
        'Non Compliant Customer'
    )
      AND (
          content_subscription_cancelled_at
          - content_subscription_activated_at
      ) > 10000
      AND content_subscription_subscription_items LIKE '%UP%'
),

payment_failed AS (
    SELECT
        content_customer_id::VARCHAR AS user_id,
        DATE(DATEADD(HOUR, -5, timestamp)) AS payment_failed_report_date,
        timestamp AS payment_failed_timestamp,
        content_transaction_error_code
    FROM chargebee_webhook_events.payment_failed
),

joined AS (
    SELECT
        c.*,
        p.payment_failed_report_date,
        p.content_transaction_error_code,
        ROW_NUMBER() OVER (
            PARTITION BY
                c.user_id,
                c.report_date
            ORDER BY
                p.payment_failed_report_date DESC,
                p.payment_failed_timestamp DESC
        ) AS rn
    FROM cancelled c
    LEFT JOIN payment_failed p
        ON c.user_id = p.user_id
       AND p.payment_failed_report_date >= DATEADD(DAY, -15, c.report_date)
AND p.payment_failed_report_date < c.report_date
)

SELECT
    user_id,
    billing_period,
    report_date,
    platform,
    gateway,
    cancel_reason,
    trialist,
    payment_failed_report_date,
    content_transaction_error_code
FROM joined
WHERE rn = 1
  ;;
  }

  dimension: user_id {
    type: string

    sql: ${TABLE}.user_id ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
  }


  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: cancel_reason {
    type: string
    sql: ${TABLE}.content_transaction_error_code ;;
  }

  dimension: gateway {
    type: string
    sql: ${TABLE}.gateway ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

    dimension: trialist {
      type: string
      sql: ${TABLE}.trialist ;;
    }

  measure: churn_count {
    type: count_distinct
    sql:${TABLE}.user_id ;;

  }
}
