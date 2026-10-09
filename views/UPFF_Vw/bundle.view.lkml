view: bundle {
  derived_table: {
    sql:

    with bundles as (
      SELECT
      content_customer_email AS email,
     DATE(DATEADD(hour, -4, timestamp)) AS report_date,
    content_subscription_billing_period_unit as billing_period,
      LISTAGG(DISTINCT content_subscription_subscription_items_0_item_price_id, ', ')
          WITHIN GROUP (ORDER BY content_subscription_subscription_items_0_item_price_id) AS products
      FROM chargebee_webhook_events.subscription_created
      GROUP BY 1,2,3
      ),

    classify_bundle as (
    SELECT
      email
      ,report_date
      ,billing_period
      ,CASE WHEN products ILIKE '%UP%' THEN 1 ELSE 0 END AS has_upff,
      CASE WHEN products ILIKE '%Gaither%' THEN 1 ELSE 0 END AS has_gaither,
      CASE WHEN products ILIKE '%Minno%' THEN 1 ELSE 0 END AS has_minno
    FROM bundles
    )

    SELECT
      email
      ,report_date
      ,lower(billing_period) as billing_period
      ,CASE
        WHEN has_upff = 1 and has_gaither = 1 and has_minno = 1 THEN 'UP Entertainment Bundle'
        WHEN has_upff = 1 and has_gaither = 1 and has_minno = 0 THEN 'GaitherTV+ Bundle'
        WHEN has_upff = 1 and has_gaither = 0 and has_minno = 1 THEN 'Minno Bundle'
        WHEN has_upff = 0 and has_gaither = 1 and has_minno = 1 THEN 'GaitherTV+ Minno Bundle'
        ELSE 'No Bundle'
      END AS bundle_type
    FROM classify_bundle
    ;;
  }

  dimension: bundle_type  {
    type: string
    sql: ${TABLE}.bundle_type ;;
  }
  dimension:email  {
    type: string
    sql: ${TABLE}.email ;;
  }
  dimension: billing_period  {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
  }
  }
