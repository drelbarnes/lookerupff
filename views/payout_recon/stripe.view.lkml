view: stripe {
  derived_table: {
    sql: with cfg AS (  -- renamed from "cfg"
  SELECT report_date
  FROM ${config.SQL_TABLE_NAME}),

 paypal as (
SELECT distinct
customer_email as email
, date(charge_created) as charge_created
, 'charge' as reporting_category
, source_id
, balance_transaction_id as transaction_id
, gross
,  fee
, 'paypal' as payment_gateway
,reporting_category as payment_description
FROM  `up-faith-and-family-216419.customers.stripe_payout_recon_4_2026_exact_match`
--FROM  `up-faith-and-family-216419.customers.paypal_payout_recon_3_2026`
WHERE date(charge_created) between (SELECT report_date FROM config) - INTERVAL 31 DAY
  AND (SELECT report_date FROM config)),

paypal_chargebee as (
SELECT * FROM paypal
WHERE payment_description in ('Payment Refund','PreApproved Payment Bill User Payment')),

paypal_non_chargebee as (SELECT * FROM paypal
WHERE payment_description not in ('Payment Refund','PreApproved Payment Bill User Payment')),

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
  ,content_customer_payment_method_reference_id
  --'charge' AS reporting_category
 from `up-faith-and-family-216419.chargebee_webhook_events.payment_succeeded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 31 DAY
  AND (SELECT report_date FROM config)),

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
  content_invoice_amount_paid as total_amount,
  content_customer_payment_method_reference_id
  --'refund' AS reporting_category
FROM
 `up-faith-and-family-216419.chargebee_webhook_events.payment_refunded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 31 DAY
  AND (SELECT report_date FROM config)
 /*
 UNION ALL

 SELECT
  upper(SUBSTR(content_credit_note_billing_address_first_name, 1, 1)) as first_initial, -- First Name
  upper(SUBSTR(content_credit_note_billing_address_last_name, 1, 1)) as last_initial, -- Last Name
  content_credit_note_customer_id as content_transaction_customer_id,
  NULL as content_customer_email,
  content_transaction_id_at_gateway,
  received_at,
  content_credit_note_status,
  content_credit_note_line_items_0_entity_id AS entity_id,


 FROM
 `up-faith-and-family-216419.chargebee_webhook_events.credit_note_created` WHERE date(received_at) between start_date AND end_date
 */
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
  ,p.transaction_id
  ,p.source_id as ref_id
  ,p.gross
  ,p.fee
 from paypal_chargebee p
left join chargebee_transactions c on p.transaction_id = c.transaction_id
--or p.source_id = c.content_customer_payment_method_reference_id
),

charge_refund as (

  SELECT * FROM
  chargebee_transactions

),

fill_non_chargebee as (
SELECT
  c.customer_id
  ,c.email
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
  ,p.transaction_id
  ,p.source_id as ref_id
  ,p.gross
  ,p.fee
  FROM paypal_non_chargebee p
  LEFT JOIN charge_refund c
  ON c.transaction_id = p.source_id
),

result as (
SELECT * FROM fill_chargebee
UNION ALL
SELECT * FROM fill_non_chargebee
)



   select distinct * from result  ;;
  }
}
