# Shared base for chargebee_webhook_events.subscription_cancelled.
#
# One row per cancelled webhook row. There are no row filters and no dedup:
# each consumer keeps its own WHERE clause and picks the date column it uses.
#
# Keep this view EPHEMERAL (no sql_trigger_value, datagroup_trigger or
# persist_for). Looker then inlines it as a CTE named chargebee_cancellations
# in front of the consumer's SQL. That is what the leading-comma PDTs in this
# folder (rolling_platform, churn_gain, ...) rely on, and it adds no rebuild
# dependency between PDTs.
#
# Columns keep the raw source semantics (NULLs included), so a consumer can swap
#   FROM chargebee_webhook_events.subscription_cancelled
# for
#   FROM ${chargebee_cancellations.SQL_TABLE_NAME}
# and translate its expressions one for one without changing any numbers.

view: chargebee_cancellations {
  derived_table: {
    sql:
      SELECT
        -- identifiers
        content_subscription_id::VARCHAR                           AS subscription_id,
        content_customer_id                                        AS customer_id,
        content_customer_email                                     AS customer_email,

      -- plan
      content_subscription_billing_period_unit                   AS billing_period_unit,
      CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
      END                                                        AS billing_period,
      content_subscription_subscription_items                    AS subscription_items,
      (content_subscription_subscription_items LIKE '%UP%')      AS is_up,
      COALESCE(
      content_subscription_subscription_items_0_item_price_id::VARCHAR,
      json_extract_path_text(
      json_extract_array_element_text(content_subscription_subscription_items, 0),
      'item_price_id'
      )::VARCHAR
      )                                                          AS plan_id,

      -- cancellation
      content_subscription_cancel_reason_code                    AS cancel_reason_code,
      -- TRUE for the 7 dunning (involuntary) codes, FALSE otherwise (a NULL
      -- reason is FALSE). So NOT is_dunning matches the existing
      --   (code NOT IN (...) OR code IS NULL)
      -- This is the single copy of the list: consumers use is_dunning
      -- instead of repeating the codes.
      COALESCE(
      content_subscription_cancel_reason_code IN (
      'Not Paid', 'No Card', 'Fraud Review Failed',
      'Non Compliant EU Customer', 'Tax Calculation Failed',
      'Currency incompatible with Gateway', 'Non Compliant Customer'
      ),
      FALSE
      )                                                          AS is_dunning,
      source,
      (source = 'admin_console')                                 AS is_admin_cancel,
      content_card_gateway                                       AS card_gateway,

      -- event clocks
      "timestamp"                                                AS event_ts,
      received_at,
      DATE("timestamp")                                          AS event_date_utc,
      DATE(DATEADD(HOUR, -4, "timestamp"))                      AS event_date_est,
      DATE(DATEADD(HOUR, +18, "timestamp"))                      AS event_date_plus18,

      -- subscription lifecycle, epoch seconds as stored by Chargebee
      content_subscription_cancelled_at                          AS cancelled_at_epoch,
      content_subscription_activated_at                          AS activated_at_epoch,
      content_subscription_created_at                            AS subscription_created_at_epoch,
      content_subscription_trial_end                             AS trial_end_epoch,
      content_customer_created_at                                AS customer_created_at_epoch,
      (content_subscription_cancelled_at - content_subscription_activated_at) AS secs_since_activation,
      (content_subscription_cancelled_at - content_subscription_trial_end)    AS secs_since_trial_end,
      (content_subscription_cancelled_at - content_subscription_created_at)   AS secs_since_created
      FROM chargebee_webhook_events.subscription_cancelled
      ;;
  }
}
