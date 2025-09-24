view: chargebee_webhook {
  derived_table: {
    sql: WITH event_mapping AS (
  /* CUSTOMER CREATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_created' AS event,
    "timestamp"::TIMESTAMP AS "timestamp"
  FROM chargebee_webhook_events.customer_created

  UNION ALL
  /* CUSTOMER DELETED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_deleted' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.customer_deleted

  UNION ALL
  /* CUSTOMER UPDATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_updated' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.customer_changed

  UNION ALL
  /* CUSTOMER PRODUCT CREATED / FREE TRIAL CREATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN event = 'subscription_created' AND content_subscription_status = 'in_trial'
        THEN 'customer_product_free_trial_created'
      ELSE 'customer_product_created'
    END AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_created

  UNION ALL
  /* FREE TRIAL CONVERTED (no dunning) */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_free_trial_converted' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_due_invoices_count = 0

  UNION ALL
  /* FREE TRIAL CONVERTED (dunning) */
  SELECT
    a.content_customer_email::VARCHAR AS email,
    'customer_product_free_trial_converted' AS event,
    a."timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_activated a
  INNER JOIN chargebee_webhook_events.payment_succeeded b
    ON a.content_invoice_id = b.content_invoice_id
  WHERE a.content_subscription_due_invoices_count >= 1

  UNION ALL
  /* PRODUCT CREATED (REACQUISITION) */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_created' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_reactivated

  UNION ALL
  /* PRODUCT RENEWED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_renewed' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_renewed

  UNION ALL
  /* PRODUCT CANCELLED / FREE TRIAL EXPIRED */
  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
        THEN 'customer_product_free_trial_expired'
      ELSE 'customer_product_cancelled'
    END AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE (
    content_subscription_cancel_reason_code NOT IN (
      'Not Paid','No Card','Fraud Review Failed','Non Compliant EU Customer',
      'Tax Calculation Failed','Currency incompatible with Gateway','Non Compliant Customer'
    )
    OR content_subscription_cancel_reason_code IS NULL
  )

  UNION ALL
  /* PRODUCT CANCELLED / FREE TRIAL EXPIRED (DUNNING) */
  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN (content_subscription_cancelled_at - content_subscription_activated_at) < 2419200
        THEN 'customer_product_free_trial_expired'
      ELSE 'customer_product_cancelled'
    END AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE content_subscription_cancel_reason_code IN (
    'Not Paid','No Card','Fraud Review Failed','Non Compliant EU Customer',
    'Tax Calculation Failed','Currency incompatible with Gateway','Non Compliant Customer'
  )

  UNION ALL
  /* PRODUCT PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_paused' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_paused

  UNION ALL
  /* PRODUCT RESUMED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_resumed' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_resumed

  UNION ALL
  /* PRODUCT CHARGE FAILED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_charge_failed' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.payment_failed

  UNION ALL
  /* PRODUCT SET CANCELLATION */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_set_cancellation' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_cancellation_scheduled

  UNION ALL
  /* PRODUCT UNDO SET CANCELLATION */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_undo_set_cancellation' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_scheduled_cancellation_removed

  UNION ALL
  /* PRODUCT SET PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_set_paused' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_pause_scheduled

  UNION ALL
  /* PRODUCT UNDO SET PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_product_undo_set_paused' AS event,
    "timestamp"::TIMESTAMP
  FROM chargebee_webhook_events.subscription_scheduled_pause_removed
)
SELECT email, event, "timestamp"
FROM (
  SELECT
    email,
    event,
    "timestamp",
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY "timestamp" DESC) AS rn
  FROM event_mapping
) s
WHERE rn = 1

;;

  }
}
