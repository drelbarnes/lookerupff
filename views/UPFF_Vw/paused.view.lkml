view: paused {
  derived_table: {
    sql:
    WITH one_paused AS (
          SELECT DISTINCT content_customer_id
          FROM chargebee_webhook_events.subscription_paused
          WHERE DATE(timestamp) >= '2025-07-01'
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
          FROM chargebee_webhook_events.subscription_resumed
          WHERE DATE(timestamp) > '2025-07-01'
      ),

      revenue AS (
          SELECT
              content_customer_id,
              SUM(content_invoice_sub_total) AS total_revenue
          FROM chargebee_webhook_events.payment_succeeded
          WHERE content_customer_id in (SELECT content_customer_id FROM no_bundles)
          GROUP BY content_customer_id
      )

      SELECT
          nb.content_customer_id,
          CASE
              WHEN r.content_customer_id IS NOT NULL THEN 'Yes'
              ELSE 'No'
          END AS resumed,
          COALESCE(rev.total_revenue, 0) AS total_revenue
      FROM no_bundles nb
      LEFT JOIN resumed r
        ON nb.content_customer_id = r.content_customer_id
      LEFT JOIN revenue rev
        ON nb.content_customer_id = rev.content_customer_id
    ;;
  }

  dimension: customer_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.content_customer_id ;;
  }

  dimension: resumed {
    type: yesno
    sql: ${TABLE}.resumed = 'Yes' ;;
  }

  dimension: resumed_label {
    type: string
    sql: ${TABLE}.resumed ;;
  }

  dimension: total_revenue {
    type: number
    value_format_name: usd
    sql: ${TABLE}.total_revenue ;;
  }

  ### =====================
  ### Measures (Optional but useful)
  ### =====================

  measure: customers {
    type: count
  }

  measure: resumed_customers {
    type: count
    filters: [resumed: "yes"]
  }

  measure: resumed_ratio {
    type: number
    value_format_name: percent_2
    sql: ${resumed_customers} * 1.0 / NULLIF(${customers}, 0) ;;
  }

  measure: avg_revenue_per_customer {
    type: average
    value_format_name: usd
    sql: ${total_revenue} ;;
  }

  measure: total_revenue_sum {
    type: sum
    value_format_name: usd
    sql: ${total_revenue} ;;
  }
  }
