view: bundle_conversion {
  derived_table: {
    sql:
  with conversion_m as (
    SELECT
      content_customer_email as email
      ,content_subscription_billing_period_unit as billing_period
      ,date(timestamp) as activation_date
      ,'minno' as product
    FROM chargebee_webhook_events.subscription_activated
    WHERE content_subscription_subscription_items like '%Mi%'),

  conversion_g as (
    SELECT
      content_customer_email as email
      ,content_subscription_billing_period_unit as billing_period
      ,date(timestamp) as activation_date
      ,'gather' as product
    FROM chargebee_webhook_events.subscription_activated
    WHERE content_subscription_subscription_items like '%Ga%'),

  conversion as (
    SELECT
      m.email
      ,m.product as minno_product
      ,g.product as gaither_product
      ,m.activation_date
      ,m.billing_period
    FROM conversion_m as m
    LEFT JOIN conversion_g g
    ON m.email = g.email and m.activation_date = g.activation_date and m.billing_period = g.billing_period),

  bundles as (
    SELECT
      email
      ,report_date as trial_start_date
      ,bundle_type
      ,billing_period
    FROM  ${bundle.SQL_TABLE_NAME}
    WHERE bundle_type = 'UP Entertainment Bundle'
  )

  SELECT
    b.email
    ,b.trial_start_date
    ,b.billing_period
    ,c.activation_date
    ,c.minno_product
    ,c.gaither_product
    ,CASE
      WHEN b.billing_period = 'year' THEN trial_start_date + 8
      WHEN b.billing_period = 'month' and b.trial_start_date >= '2026-09-22' THEN trial_start_date + 30
      ELSE trial_start_date + 8
    END AS expected_conversion_date
  FROM bundles b
  LEFT JOIN conversion c
  ON b.email = c.email
    ;;
  }

  dimension: activation_date {
    type: date
    sql: ${TABLE}.activation_date ;;
  }

  dimension: trial_start_date {
    type: date
    sql: ${TABLE}.trial_start_date ;;
  }

  dimension: expected_conversion_date {
    type: date
    sql: ${TABLE}.expected_conversion_date ;;
  }


  dimension:email  {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension:billing_period  {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension:gaither_product  {
    type: string
    sql: ${TABLE}.gaither_product ;;
  }

  dimension:minno_product  {
    type: string
    sql: ${TABLE}.minno_product ;;
  }


}
