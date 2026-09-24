view: bundle {
  derived_table: {
    sql:

    with bundles as (
      SELECT
      content_customer_email AS email,
     DATE(DATEADD(hour, -4, timestamp)) AS report_date,
      LISTAGG(DISTINCT content_subscription_subscription_items_0_item_price_id, ', ')
          WITHIN GROUP (ORDER BY content_subscription_subscription_items_0_item_price_id) AS products
      FROM chargebee_webhook_events.subscription_created
      GROUP BY 1,2
      ),

    classify_bundle as (
    SELECT
      email
      ,report_date
      ,CASE WHEN products ILIKE '%UP%' THEN 1 ELSE 0 END AS has_upff,
      CASE WHEN products ILIKE '%Gaither%' THEN 1 ELSE 0 END AS has_gaither,
      CASE WHEN products ILIKE '%Minno%' THEN 1 ELSE 0 END AS has_minno
    FROM bundles
    )

    SELECT
      email
      ,report_date
      ,CASE
        WHEN has_upff = 1 and has_gaither = 1 and has_minno = 1 THEN 'UP Entertainment Bundle'
        WHEN has_upff = 1 and has_gaither = 1 and has_minno = 0 THEN 'GaitherTV+ Bundle'
        WHEN has_upff = 1 and has_gaither = 0 and has_minno = 1 THEN 'Minno Bundle'
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


  dimension: date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${TABLE}.email ;;
  }
  }
