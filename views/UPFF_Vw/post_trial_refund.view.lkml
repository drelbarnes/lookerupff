view: post_trial_refund {
  derived_table: {
    sql: WITH cancel AS (
  SELECT
    content_customer_id AS customer_id,
    received_at           AS cancelled_at
    ,TIMESTAMP 'epoch' + content_subscription_trial_end * INTERVAL '1 second' AS activated_at
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE source = 'admin_console'
    AND content_subscription_subscription_items LIKE '%UP%'
),

activated AS (
  SELECT
    content_customer_id AS customer_id,
    received_at            AS activated_at
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items LIKE '%UP%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY received_at) = 1
),

payment AS (
  SELECT
    content_customer_id AS customer_id,
    received_at            AS paid_at
  FROM chargebee_webhook_events.payment_succeeded
  WHERE content_subscription_subscription_items LIKE '%UP%'
),

refund AS (
  SELECT
   content_credit_note_customer_id AS customer_id,
    received_at            AS refunded_at
  FROM chargebee_webhook_events.credit_note_created
  WHERE content_credit_note_line_items_0_description LIKE '%UP%'
  and content_credit_note_type = 'refundable'
),

/* First payment after activation (per user) */
first_payment AS (
  SELECT
    a.customer_id,
    a.activated_at,
    MIN(p.paid_at) AS paid_at
  FROM activated a
  LEFT JOIN payment p
    ON p.customer_id = a.customer_id
   AND p.paid_at > a.activated_at
  GROUP BY 1,2
),

/* First cancel after that payment, but within 1 month of activation */
first_cancel AS (
  SELECT
    fp.customer_id,
    fp.activated_at,
    fp.paid_at,
    MIN(c.cancelled_at) AS cancelled_at
  FROM first_payment fp
  LEFT JOIN cancel c
    ON c.customer_id = fp.customer_id
   AND c.cancelled_at > fp.paid_at
   AND c.cancelled_at <= DATEADD('month', 1, fp.activated_at)
  GROUP BY 1,2,3
),

/* Refund occurring on the SAME DATE as the cancel */
refund_on_cancel_date AS (
  SELECT
    fc.customer_id,
    fc.activated_at,
    fc.paid_at,
    fc.cancelled_at,
    MIN(r.refunded_at) AS refunded_at
  FROM first_cancel fc
  LEFT JOIN refund r
    ON r.customer_id = fc.customer_id
   AND DATE(r.refunded_at) = DATE(fc.cancelled_at)
  GROUP BY 1,2,3,4
)

/* Final sequences that meet all criteria */
SELECT
  customer_id,
  activated_at,
  paid_at,
  cancelled_at,
  refunded_at
FROM refund_on_cancel_date
WHERE paid_at IS NOT NULL
  AND cancelled_at IS NOT NULL
 AND refunded_at IS NOT NULL
;;  }
  dimension: activated_at {
    type:  date
    sql: ${TABLE}. activated_at ;;
  }

  dimension: paid_at {
    type:  date
    sql: ${TABLE}. paid_at ;;
  }


  dimension:cancelled_at {
    type:  date
    sql: ${TABLE}. cancelled_at ;;
  }
  dimension:refunded_at {
    type:  date
    sql: ${TABLE}. refunded_at ;;
  }



  dimension: customer_id {
    type:  string
    sql: ${TABLE}.customer_id;;
  }



  measure: total {
    type: count_distinct
    sql: ${TABLE}.customer_id  ;;
  }
}
