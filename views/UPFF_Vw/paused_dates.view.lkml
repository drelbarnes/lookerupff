view: paused_dates {
derived_table: {
  sql: WITH one_paused AS (
          SELECT DISTINCT content_customer_id
          ,date(timestamp) as paused_date
          FROM chargebee_webhook_events.subscription_paused
          WHERE DATE(timestamp) >= '2025-07-01' and content_subscription_subscription_items like '%UP%'
      ),

      bundles_consolidated AS (
          SELECT DISTINCT content_customer_id
          FROM chargebee_webhook_events.payment_succeeded
          WHERE content_customer_id IN (SELECT content_customer_id FROM one_paused)
            AND DATE(timestamp) > '2025-10-01'
            AND content_invoice_line_items_0_entity_id IS NOT NULL
            AND content_invoice_line_items_1_entity_id IS NOT NULL
      ),

      bundles AS (
          SELECT
              COUNT(*) AS cnt,
              content_customer_id,
              DATE(timestamp) AS report_date
          FROM chargebee_webhook_events.payment_succeeded
          WHERE content_customer_id IN (SELECT content_customer_id FROM one_paused)
          GROUP BY content_customer_id, report_date
      ),

      bundles2 AS (
          SELECT DISTINCT content_customer_id
          FROM bundles
          WHERE cnt > 1
      ),

      no_bundles AS (
          SELECT content_customer_id
          FROM one_paused
          WHERE content_customer_id NOT IN (SELECT content_customer_id FROM bundles2)
            AND content_customer_id NOT IN (SELECT content_customer_id FROM bundles_consolidated)
      ),

      resumed AS (
          SELECT DISTINCT content_customer_id
          ,date(timestamp) as resume_date
          FROM chargebee_webhook_events.subscription_resumed
          WHERE DATE(timestamp) > '2025-07-01'
      )

      SELECT
    p.content_customer_id,
    p.paused_date,
    r.resume_date,
    DATEDIFF(day, p.paused_date, r.resume_date) AS days_paused
FROM one_paused p
JOIN resumed r
    ON p.content_customer_id = r.content_customer_id
    AND r.resume_date >= p.paused_date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.content_customer_id, p.paused_date
    ORDER BY r.resume_date
) = 1 ;;
}

  dimension: customer_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.content_customer_id ;;
  }
  dimension: paused_date {
    type: date
    sql: ${TABLE}.paused_date ;;
  }

  dimension: resumed_date {
    type: date
    sql: ${TABLE}.resumed_date ;;
  }

  dimension: days_paused {
    type: number
    sql: ${TABLE}.days_paused ;;
  }

  measure:_average_days_paused {
    type: average
    sql: ${TABLE}.days_paused ;;
  }
}
