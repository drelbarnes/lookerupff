view: paypal_old {
  derived_table: {
    sql: -- Declare variables for start and end dates


      with cfg AS (  -- renamed from "cfg"
  SELECT report_date
  FROM ${config.SQL_TABLE_NAME}),

  paypal as (
SELECT distinct
To_Email_Address as email
, date(_Date_) as charge_created
, 'charge' as reporting_category
, Reference_Txn_ID as source_id
, Transaction_ID as transaction_id
, Gross
, fee
, 'paypal' as payment_gateway
, type as payment_description
FROM `up-faith-and-family-216419.customers.paypal_recon_payout_feb_2026`   WHERE date(_Date_) between (SELECT report_date FROM config) - INTERVAL 31 DAY - INTERVAL 1 MONTH
  AND (SELECT report_date FROM config)- INTERVAL 1 MONTH

  UNION ALL
  SELECT distinct
To_Email_Address as email
, date(_Date_) as charge_created
, 'charge' as reporting_category
, Reference_Txn_ID as source_id
, Transaction_ID as transaction_id
, Gross
, fee
, 'paypal' as payment_gateway
, type as payment_description
FROM `up-faith-and-family-216419.customers.paypal_payout_recon_2_2026`  WHERE date(_Date_) between (SELECT report_date FROM config) - INTERVAL 31 DAY - INTERVAL 2 MONTH
  AND (SELECT report_date FROM config)- INTERVAL 2 MONTH

  ),

paypal_chargebee as (
SELECT * FROM paypal
WHERE payment_description in ('Payment Refund','PreApproved Payment Bill User Payment')),


charges as (SELECT distinct
  content_transaction_customer_id as customer_id,
  content_customer_email as email,
  content_transaction_id_at_gateway as transaction_id,
  date(received_at) as report_date,
  CASE
    WHEN content_invoice_line_items_0_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_1,
  CASE
    WHEN content_invoice_line_items_1_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_2,
  CASE
    WHEN content_invoice_line_items_2_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_3,
  CASE
    WHEN content_invoice_line_items_0_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_1_period,
  CASE
    WHEN content_invoice_line_items_1_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_2_period,
  CASE
    WHEN content_invoice_line_items_2_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_3_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  content_invoice_line_items_0_discount_amount  AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount  AS discount_amount3,
  content_invoice_amount_paid as total_amount
  --'charge' AS reporting_category
 from `up-faith-and-family-216419.chargebee_webhook_events.payment_succeeded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 31 DAY - INTERVAL 2 MONTH
  AND (SELECT report_date FROM config)- INTERVAL 1 MONTH),

 refunds as (SELECT distinct
  content_transaction_customer_id as customer_id,
  content_customer_email as email,
  content_transaction_id_at_gateway as transaction_id,
  date(received_at) as report_date,
  CASE
    WHEN content_invoice_line_items_0_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_1,
  CASE
    WHEN content_invoice_line_items_1_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_2,
  CASE
    WHEN content_invoice_line_items_2_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_3,
  CASE
    WHEN content_invoice_line_items_0_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_0_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_1_period,
  CASE
    WHEN content_invoice_line_items_1_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_1_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_2_period,
  CASE
    WHEN content_invoice_line_items_2_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_2_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_3_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  content_invoice_line_items_0_discount_amount  AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount  AS discount_amount3,
  content_invoice_amount_paid as total_amount
  --'refund' AS reporting_category
FROM
 `up-faith-and-family-216419.chargebee_webhook_events.payment_refunded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 31 DAY - INTERVAL 2 MONTH
  AND (SELECT report_date FROM config)- INTERVAL 1 MONTH
 ),


chargebee_transactions as (
SELECT * FROM charges
UNION ALL
SELECT * FROM refunds

),

 fill_chargebee as (
 select distinct
  c.customer_id
  ,c.email
  ,p.transaction_id
  ,p.charge_created as report_date
  ,p.payment_gateway
  ,p.payment_description
  ,c.product_1
  ,c.product_2
  ,c.product_3
  ,c.product_1_period
  ,c.product_2_period
  ,c.product_3_period
  ,c.original_amount1
  ,c.original_amount2
  ,c.original_amount3
  ,c.discount_amount1
  ,c.discount_amount2
  ,c.discount_amount3
  ,c.tax_1
  ,c.tax_2
  ,c.tax_3
  ,c.total_amount
  ,p.source_id as stripe_ref_id
  ,p.gross as stripe_remitted
  ,p.fee
 from paypal_chargebee p
left join chargebee_transactions c on p.transaction_id = c.transaction_id )



      select distinct * from fill_chargebee ;;
  }
}
