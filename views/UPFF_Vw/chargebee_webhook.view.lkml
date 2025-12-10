view: chargebee_webhook {
  derived_table: {
    sql: WITH event_mapping AS (
  /* CUSTOMER CREATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_created' AS event,
    "timestamp"::TIMESTAMP AS "timestamp",
    null::VARCHAR as plan
  FROM chargebee_webhook_events.customer_created

  UNION ALL
  /* CUSTOMER DELETED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_deleted' AS event,
    "timestamp"::TIMESTAMP,
    null::VARCHAR as plan
  FROM chargebee_webhook_events.customer_deleted

  UNION ALL
  /* CUSTOMER UPDATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer_updated' AS event,
    "timestamp"::TIMESTAMP,
    null::VARCHAR as plan
  FROM chargebee_webhook_events.customer_changed

  UNION ALL
  /* CUSTOMER PRODUCT CREATED / FREE TRIAL CREATED */
  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN event = 'subscription_created' AND content_subscription_status = 'in_trial'
        THEN 'customer.product.free_trial_created'
      ELSE 'customer.product.created'
    END AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_created

  UNION ALL
  /* FREE TRIAL CONVERTED (no dunning) */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.free_trial_converted' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_due_invoices_count = 0

  UNION ALL
  /* FREE TRIAL CONVERTED (dunning) */
  SELECT
    a.content_customer_email::VARCHAR AS email,
    'customer.product.free_trial_converted' AS event,
    a."timestamp"::TIMESTAMP,
    COALESCE(
      a.content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(a.content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_activated a
  INNER JOIN chargebee_webhook_events.payment_succeeded b
    ON a.content_invoice_id = b.content_invoice_id
  WHERE a.content_subscription_due_invoices_count >= 1

  UNION ALL
  /* PRODUCT CREATED (REACQUISITION) */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.created' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_reactivated

  UNION ALL
  /* PRODUCT RENEWED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.renewed' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_renewed

  UNION ALL
  /* PRODUCT CANCELLED / FREE TRIAL EXPIRED */
  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
        THEN 'customer.product.free_trial_expired'
      ELSE 'customer.product.cancelled'
    END AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
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
        THEN 'customer.product.free_trial_expired'
      ELSE 'customer.product.cancelled'
    END AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE content_subscription_cancel_reason_code IN (
    'Not Paid','No Card','Fraud Review Failed','Non Compliant EU Customer',
    'Tax Calculation Failed','Currency incompatible with Gateway','Non Compliant Customer'
  )

  UNION ALL
  /* PRODUCT PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.paused' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_paused

  UNION ALL
  /* PRODUCT RESUMED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.resumed' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_resumed

  UNION ALL
  /* PRODUCT CHARGE FAILED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.charge_failed' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.payment_failed

  UNION ALL
  /* PRODUCT SET CANCELLATION */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.set_cancellation' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_cancellation_scheduled

  UNION ALL
  /* PRODUCT UNDO SET CANCELLATION */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.undo_set_cancellation' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_scheduled_cancellation_removed

  UNION ALL
  /* PRODUCT SET PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.set_paused' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_pause_scheduled

  UNION ALL
  /* PRODUCT UNDO SET PAUSED */
  SELECT
    content_customer_email::VARCHAR AS email,
    'customer.product.undo_set_paused' AS event,
    "timestamp"::TIMESTAMP,
    COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.subscription_scheduled_pause_removed

  UNION ALL

  SELECT
  content_customer_email as email,
  CASE
    WHEN occurred_at - content_customer_created_at > 2419200 then 'customer.product.renewed'
    ELSE'customer.product.free_trial_converted'
  END AS event,
  "timestamp"::TIMESTAMP,
  COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
        json_extract_array_element_text(content_subscription_subscription_items, 0),
        'item_price_id'
      )::VARCHAR
    ) AS plan
  FROM chargebee_webhook_events.payment_succeeded where content_invoice_dunning_status is not NULL
)




SELECT email, event, "timestamp", plan
FROM (
  SELECT
    lower(email) as email,
    event,
    "timestamp",
    plan,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY "timestamp" DESC) AS rn
  FROM event_mapping
  WHERE (plan not like 'Min%' and plan not like 'Gaither%') or plan is NULL
) s
WHERE rn = 1

;;

  }
}
