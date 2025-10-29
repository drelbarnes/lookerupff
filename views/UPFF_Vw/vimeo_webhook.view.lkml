view: vimeo_webhook {
  derived_table: {
    sql: WITH purchase_events AS (
  SELECT user_id, event_text, platform, "timestamp"
  FROM vimeo_ott_webhook.customer_product_free_trial_created
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp"
  FROM vimeo_ott_webhook.customer_product_created
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp"
  FROM vimeo_ott_webhook.customer_created
  UNION ALL
  SELECT user_id, event_text, platform, dateadd(minute, 1, "timestamp") AS "timestamp"
  FROM vimeo_ott_webhook.customer_product_expired
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_free_trial_converted
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_free_trial_expired
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_charge_failed
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_renewed
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_set_cancellation
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_cancelled
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_resumed
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_updated
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_paused
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_disabled
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_undo_set_paused
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_set_paused
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_product_undo_set_cancellation
  UNION ALL
  SELECT user_id, event_text, platform, "timestamp" FROM vimeo_ott_webhook.customer_deleted
)
SELECT
  user_id,
  event_text,
  platform,
  "timestamp"
FROM purchase_events
WHERE platform != 'api' or platform is NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY "timestamp" DESC) = 1

 ;;
  }

}
