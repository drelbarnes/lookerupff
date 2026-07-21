view: paypal {
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
,type as payment_description
FROM  `up-faith-and-family-216419.customers.paypal_payout_recon_june_2026`
--FROM  `up-faith-and-family-216419.customers.paypal_payout_recon_3_2026`
WHERE date(_Date_) between (SELECT report_date FROM config) - INTERVAL 31 DAY
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
  select
  customer_id
  ,email
  ,transaction_id
  ,report_date
  ,product_1
  ,product_2
  ,product_3
  ,product_1_period
  ,product_2_period
  ,product_3_period
  ,tax_1
  ,tax_2
  ,tax_3
  ,original_amount1
  ,original_amount2
  ,original_amount3
  ,discount_amount1
  ,discount_amount2
  ,discount_amount3
  ,total_amount
  ,null as content_customer_payment_method_reference_id
  ,fee
  from ${paypal_old.SQL_TABLE_NAME}

  UNION ALL
  SELECT *,null as fee FROM
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
  ,CASE
  WHEN p.payment_description = 'Cancellation of Hold for Dispute Resolution' THEN
  c.fee
  ELSE p.fee
  end as fee
  FROM paypal_non_chargebee p
  LEFT JOIN charge_refund c
  ON c.transaction_id = p.source_id
),

result as (
SELECT * FROM fill_chargebee
UNION ALL
SELECT * FROM fill_non_chargebee
),

result2 as(
select distinct
customer_id
  ,email
  ,report_date
  ,payment_gateway
  ,payment_description
  ,product_1
  ,product_2
  ,product_3
  ,product_1_period
  ,product_2_period
  ,product_3_period
  ,CASE
    WHEN payment_description = 'Dispute Fee' THEN 0
    ELSE original_amount1
  end as original_amount1
  ,original_amount2
  ,original_amount3
  ,CASE
    WHEN payment_description = 'Dispute Fee' THEN 0
    ELSE discount_amount1
  end as discount_amount1
  ,discount_amount2
  ,discount_amount3
  ,CASE
    WHEN payment_description = 'Dispute Fee' THEN 0
    ELSE tax_1
  end as tax_1
  ,tax_2
  ,tax_3
  ,CASE
    WHEN payment_description = 'Dispute Fee' THEN -1500
    ELSE total_amount
  end as total_amount
  ,transaction_id
  ,ref_id
  ,gross
  ,CASE
    WHEN payment_description = 'Dispute Fee' THEN -15.00
    ELSE fee
  end as fee
  FROM result),

final as (
  select * from result2
  where payment_description != 'Cancellation of Hold for Dispute Resolution'
  union all
  select * from result2
  where payment_description = 'Cancellation of Hold for Dispute Resolution'
  and fee is not null)

  select *
  ,SAFE_DIVIDE(COALESCE(original_amount1, 0),
  COALESCE(original_amount1, 0)
  + COALESCE(original_amount2, 0)
  + COALESCE(original_amount3, 0)
) * fee AS fee1
,SAFE_DIVIDE(COALESCE(original_amount2, 0),
  COALESCE(original_amount1, 0)
  + COALESCE(original_amount2, 0)
  + COALESCE(original_amount3, 0)
) * fee AS fee2
,SAFE_DIVIDE(COALESCE(original_amount3, 0),
  COALESCE(original_amount1, 0)
  + COALESCE(original_amount2, 0)
  + COALESCE(original_amount3, 0)
) * fee AS fee3


  from final





    ;;
  }

    dimension: customer_id {
      type: string
      sql: ${TABLE}.customer_id ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    dimension_group: report_date {
      type: time
      timeframes: [raw, date, week, month, quarter, year]
      sql: ${TABLE}.report_date ;;
    }

    dimension: payment_gateway {
      type: string
      sql: ${TABLE}.payment_gateway ;;
    }

    dimension: payment_description {
      type: string
      sql: ${TABLE}.payment_description ;;
    }

    dimension: product_1 {
      type: string
      sql: ${TABLE}.product_1 ;;
    }

    dimension: product_2 {
      type: string
      sql: ${TABLE}.product_2 ;;
    }

    dimension: product_3 {
      type: string
      sql: ${TABLE}.product_3 ;;
    }

    dimension: product_1_period {
      type: string
      sql: ${TABLE}.product_1_period ;;
    }

    dimension: product_2_period {
      type: string
      sql: ${TABLE}.product_2_period ;;
    }

    dimension: product_3_period {
      type: string
      sql: ${TABLE}.product_3_period ;;
    }

    dimension: transaction_id {
      type: string
      primary_key: yes
      sql: ${TABLE}.transaction_id ;;
    }

    dimension: ref_id {
      type: string
      sql: ${TABLE}.ref_id ;;
    }

    dimension: original_amount1 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.original_amount1 /100.0;;
    }

    dimension: original_amount2 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.original_amount2 /100.0;;
    }

    dimension: original_amount3 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.original_amount3/100.0 ;;
    }

    dimension: discount_amount1 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.discount_amount1/100.0 ;;
    }

    dimension: discount_amount2 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.discount_amount2/100.0 ;;
    }

    dimension: discount_amount3 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.discount_amount3/100.0 ;;
    }

    dimension: tax_1 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.tax_1/100.0 ;;
    }

    dimension: tax_2 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.tax_2/100.0 ;;
    }

    dimension: tax_3 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.tax_3/100.0 ;;
    }

    dimension: total_amount {
      type: number
      value_format_name: usd
      sql: ${TABLE}.total_amount/100.0 ;;
    }

    dimension: gross {
      type: number
      value_format_name: usd
      sql: ${TABLE}.gross ;;
    }

    dimension: fee {
      type: number
      value_format_name: usd
      sql: ${TABLE}.fee ;;
    }

  dimension: fee1 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.fee1 ;;
  }

  dimension: fee2 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.fee2 ;;
  }

  dimension: fee3 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.fee3 ;;
  }
  measure: total_charge {
    type: sum
    sql: ${TABLE}.stripe_remitted ;;
  }


  }
