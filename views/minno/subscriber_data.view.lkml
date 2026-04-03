view: subscriber_data {
  derived_table:{
    sql:
    ,latest_failed AS (
  SELECT content_customer_email AS email, MAX(timestamp) AS last_failed_time
  FROM chargebee_webhook_events.payment_failed
  WHERE content_invoice_dunning_status = 'in_progress'
    AND timestamp >= current_date - interval '13 day'
    AND content_subscription_subscription_items_0_item_price_id LIKE '%Mi%'
  GROUP BY email
),
latest_succeeded AS (
  SELECT content_customer_email AS email, MAX(timestamp) AS last_success_time
  FROM chargebee_webhook_events.payment_succeeded
  WHERE timestamp >= current_date - interval '14 day'
    AND content_invoice_dunning_attempts NOT LIKE '%attempt%'
    AND content_customer_email IS NOT NULL
    AND content_subscription_subscription_items_0_item_price_id LIKE '%Mi%'
  GROUP BY email
),
chargebee AS (
  SELECT
    CAST(customer_id AS VARCHAR) AS user_id,
    customer_first_name AS first_name,
    customer_last_name AS last_name,
    LOWER(customer_email)::VARCHAR AS email,
    customer_cs_marketing_opt_in AS marketing_opt_in,
    CAST(
      CASE
        WHEN subscription_status = 'in_trial' THEN 'free_trial'
        WHEN subscription_status = 'active'  THEN 'enabled'
        ELSE subscription_status
      END
      AS VARCHAR
    ) AS subscription_status,
    CAST(
      CASE
        WHEN subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END
      AS VARCHAR
    ) AS subscription_frequency,
    CAST(subscription_subscription_items_0_item_price_id AS VARCHAR) AS plan_name,
    CAST('web' AS VARCHAR) AS platform,
    CAST(customer_billing_address_zip AS VARCHAR) AS postal_code,
    DATE(timestamp 'epoch' + subscription_started_at   * interval '1 second') AS subscription_start_date,
    DATE(timestamp 'epoch' + subscription_cancelled_at * interval '1 second') AS subscription_end_date,
    CAST(
      CASE
        WHEN subscription_status = 'active'
         AND LOWER(customer_email)::VARCHAR IN (SELECT email FROM latest_failed)
         AND LOWER(customer_email)::VARCHAR NOT IN (SELECT email FROM latest_succeeded)
        THEN 'Yes'
        ELSE 'No'
      END
      AS VARCHAR
    ) AS is_in_dunning,
    CAST(customer_billing_address_state_code AS VARCHAR) AS state,
    CASE
    WHEN LOWER(customer_email) IN (
        SELECT LOWER(customer_email)
        FROM http_api.chargebee_subscriptions
        WHERE DATE(timestamp) = CURRENT_DATE
          AND subscription_subscription_items_0_item_price_id NOT LIKE '%Min%'
    )
    THEN False
    ELSE True
END AS minno_only
  FROM http_api.chargebee_subscriptions
  WHERE subscription_subscription_items_0_item_price_id LIKE '%Mi%'
    AND DATE(timestamp) = CURRENT_DATE
),
result AS (
  SELECT * FROM chargebee
),
chargebee_events AS (
  SELECT * FROM ${chargebee_webhook.SQL_TABLE_NAME}
),
final AS (
  SELECT DISTINCT
    a.*,
    CAST(ce.event AS VARCHAR) AS topic,
    DATE(ce.timestamp) AS report_date
  FROM result a
  LEFT JOIN chargebee_events ce
    ON LOWER(TRIM(a.email)) = LOWER(TRIM(ce.email))
),
ranked_emails AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY subscription_start_date DESC) AS rn
  FROM final
)
SELECT *
FROM ranked_emails
WHERE rn = 1;;


    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -555, GETDATE()), 'YYYY-MM-DD');;
    distribution: "user_id"
    sortkeys: ["user_id"]
  }

  dimension: user_id {
    type:  string
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }

  dimension: first_name {
    type:  string
    sql: ${TABLE}.first_name ;;
  }

  dimension: report_date {
    type:  date
    sql: ${TABLE}.report_date ;;
  }

  dimension: last_name {
    type:  string
    sql: ${TABLE}.last_name ;;
  }

  dimension: state {
    type:  string
    sql: ${TABLE}.state ;;
  }

  dimension: email {
    type:  string
    sql: ${TABLE}.email;;
  }

  dimension: marketing_opt_in {
    type: yesno
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Brand"  # Optional alias
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: postal_code {
    type: string
    sql: ${TABLE}.postal_code ;;
  }

  dimension: subscription_start_date {
    type: date
    sql: ${TABLE}.subscription_start_date ;;
  }

  dimension: subscription_end_date {
    type: date
    sql: ${TABLE}.subscription_end_date ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: minno_only {
    type: yesno
    sql: ${TABLE}.minno_only ;;
  }


  measure: total {
    type: count_distinct
    sql: ${TABLE}.user_id  ;;
  }


}
