view: stripe {
  derived_table: {
    sql: -- Declare variables for start and end dates
 with cfg AS (  -- renamed from "cfg"
  SELECT report_date
  FROM ${config.SQL_TABLE_NAME}),

 paypal as (
SELECT distinct
customer_email as email
, date(charge_created) as charge_created
, 'charge' as reporting_category
, source_id as source_id
, charge_id as transaction_id
, Gross
, fee
, 'paypal' as payment_gateway
,reporting_category as payment_description
FROM  `up-faith-and-family-216419.customers.stripe_payout_recon_june_2026`
--FROM  `up-faith-and-family-216419.customers.paypal_payout_recon_3_2026`
WHERE date(charge_created) <= (SELECT report_date FROM config)),

count_dict as (
  select count(*) as count,
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
    WHEN content_invoice_line_items_3_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_4,
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
  CASE
    WHEN content_invoice_line_items_3_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_4_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  content_invoice_line_items_3_tax_amount as tax_4,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  content_invoice_line_items_3_unit_amount as original_amount4,
  content_invoice_line_items_0_discount_amount +content_invoice_credits_applied AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount AS discount_amount3,
  content_invoice_line_items_3_discount_amount AS discount_amount4,
  content_invoice_amount_paid as total_amount
  ,content_invoice_credits_applied

  from `up-faith-and-family-216419.chargebee_webhook_events.payment_succeeded`
  where date(timestamp) >='2024-07-01'
  GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
  ),
  count_dict_deduped AS (
  SELECT *
  FROM count_dict
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY total_amount
    ORDER BY count DESC
  ) = 1
),

paypal_chargebee as (
SELECT * FROM paypal
WHERE payment_description in ('charge','dispute_reversal')),

paypal_non_chargebee as (SELECT * FROM paypal
WHERE payment_description not in ('charge','dispute_reversal')),

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
    WHEN content_invoice_line_items_3_entity_id LIKE '%UP%' THEN 'UP-Faith-Family'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Minno%' THEN 'Minno'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Gaither%' THEN 'GaitherTV'
    ELSE NULL
  END AS product_4,
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
  CASE
    WHEN content_invoice_line_items_3_entity_id LIKE '%Yearly%' THEN 'Yearly'
    WHEN content_invoice_line_items_3_entity_id LIKE '%Monthly%' THEN 'Monthly'
    ELSE NULL
  END AS product_4_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  content_invoice_line_items_3_tax_amount as tax_4,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  content_invoice_line_items_3_unit_amount as original_amount4,
  content_invoice_line_items_0_discount_amount +content_invoice_credits_applied AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount AS discount_amount3,
  content_invoice_line_items_3_discount_amount AS discount_amount4,
  content_invoice_amount_paid as total_amount
  ,content_customer_payment_method_reference_id
  ,content_invoice_credits_applied
  --'charge' AS reporting_category
 from `up-faith-and-family-216419.chargebee_webhook_events.payment_succeeded` ),

 refunds as  (SELECT distinct
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
  cast(NULL as string) AS product_4,
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
  cast(NULL as string) AS product_4_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  NULL as tax_4,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  NULL as original_amount4,
  content_invoice_line_items_0_discount_amount +content_invoice_credits_applied AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount AS discount_amount3,
  NULL AS discount_amount4,
  content_invoice_amount_paid as total_amount,
  content_customer_payment_method_reference_id
  ,content_invoice_credits_applied
  --'refund' AS reporting_category
FROM
 `up-faith-and-family-216419.chargebee_webhook_events.payment_refunded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 31 DAY
  AND (SELECT report_date FROM config) and content_invoice_issued_credit_notes_0_cn_reason_code != 'subscription_change'

  union all

  SELECT distinct
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
  cast(NULL as string) AS product_4,
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
  cast(NULL as string) AS product_4_period,
  content_invoice_line_items_0_tax_amount as tax_1,
  content_invoice_line_items_1_tax_amount as tax_2,
  content_invoice_line_items_2_tax_amount as tax_3,
  NULL as tax_4,
  content_invoice_line_items_0_unit_amount as original_amount1,
  content_invoice_line_items_1_unit_amount as original_amount2,
  content_invoice_line_items_2_unit_amount as original_amount3,
  NULL as original_amount4,
  content_invoice_line_items_0_discount_amount +content_invoice_credits_applied AS discount_amount1,
  content_invoice_line_items_1_discount_amount  AS discount_amount2,
  content_invoice_line_items_2_discount_amount AS discount_amount3,
  NULL AS discount_amount4,
  content_invoice_amount_paid as total_amount,
  content_customer_payment_method_reference_id
  ,content_invoice_credits_applied
  --'refund' AS reporting_category
FROM
 `up-faith-and-family-216419.chargebee_webhook_events.payment_refunded` WHERE date(received_at) between (SELECT report_date FROM config) - INTERVAL 34 DAY
  AND (SELECT report_date FROM config) and content_invoice_issued_credit_notes_0_cn_reason_code = 'subscription_change'
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
  ,c.product_4
  ,c.product_1_period
  ,c.product_2_period
  ,c.product_3_period
  ,c.product_4_period
  ,c.original_amount1
  ,c.original_amount2
  ,c.original_amount3
  ,c.original_amount4
  ,c.discount_amount1
  ,c.discount_amount2
  ,c.discount_amount3
  ,c.discount_amount4
  ,c.tax_1
  ,c.tax_2
  ,c.tax_3
  ,c.tax_4
  ,c.total_amount
  ,p.transaction_id
  ,p.source_id as ref_id
  ,p.gross
  ,p.fee
  ,c.content_invoice_credits_applied
 from paypal_chargebee p
left join chargebee_transactions c on p.transaction_id = c.transaction_id
--or p.source_id = c.content_customer_payment_method_reference_id
),

charges_not_filled as (
select * from fill_chargebee
where total_amount is NULL or( cast(total_amount/100.0 * 1.0 as string) != cast(gross as string))
),
fill_charge_not_filled as (
SELECT
  cast(NULL as string) AS customer_id
  ,cast(NULL as string) AS email
  ,p.report_date
  ,p.payment_gateway
  ,p.payment_description
  ,c.product_1
  ,c.product_2
  ,c.product_3
  ,c.product_4
  ,c.product_1_period
  ,c.product_2_period
  ,c.product_3_period
  ,c.product_4_period
  ,c.original_amount1
  ,c.original_amount2
  ,c.original_amount3
  ,c.original_amount4
  ,c.discount_amount1
  ,c.discount_amount2
  ,c.discount_amount3
  ,c.discount_amount4
  ,c.tax_1
  ,c.tax_2
  ,c.tax_3
  ,c.tax_4
  ,c.total_amount
  ,p.transaction_id
  ,p.ref_id
  ,p.gross
  ,p.fee
  ,c.content_invoice_credits_applied
  FROM charges_not_filled p
  LEFT JOIN count_dict_deduped c
  ON cast(c.total_amount/100.0 * 1.0 as string)= cast(p.gross as string)),


charge_refund as (

  SELECT * FROM
  refunds

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
  ,c.product_4
  ,c.product_1_period
  ,c.product_2_period
  ,c.product_3_period
  ,c.product_4_period
  ,c.original_amount1
  ,c.original_amount2
  ,c.original_amount3
  ,c.original_amount4
  ,c.discount_amount1
  ,c.discount_amount2
  ,c.discount_amount3
  ,c.discount_amount4
  ,c.tax_1
  ,c.tax_2
  ,c.tax_3
  ,c.tax_4
  ,c.total_amount
  ,p.transaction_id
  ,p.source_id as ref_id
  ,p.gross
  ,p.fee
  ,c.content_invoice_credits_applied
  FROM paypal_non_chargebee p
  LEFT JOIN charge_refund c
  ON c.transaction_id = p.source_id
),

refund_not_filled as (
select * from fill_non_chargebee
where total_amount is NULL or( cast(total_amount/100.0 * -1.0 as string) != cast(gross as string))
),

fill_not_filled as (
SELECT
  cast(NULL as string) AS customer_id
  ,cast(NULL as string) AS email
  ,p.report_date
  ,p.payment_gateway
  ,p.payment_description
  ,c.product_1
  ,c.product_2
  ,c.product_3
  ,c.product_4
  ,c.product_1_period
  ,c.product_2_period
  ,c.product_3_period
  ,c.product_4_period
  ,c.original_amount1
  ,c.original_amount2
  ,c.original_amount3
  ,c.original_amount4
  ,c.discount_amount1
  ,c.discount_amount2
  ,c.discount_amount3
  ,c.discount_amount4
  ,c.tax_1
  ,c.tax_2
  ,c.tax_3
  ,c.tax_4
  ,c.total_amount
  ,p.transaction_id
  ,p.ref_id
  ,p.gross
  ,p.fee
  ,c.content_invoice_credits_applied
  FROM refund_not_filled p
  LEFT JOIN count_dict_deduped c
  ON cast(c.total_amount/100.0 * -1.0 as string)= cast(p.gross as string)),


result as (
SELECT * FROM fill_chargebee
where total_amount is not NULL and ( cast(total_amount/100.0 * 1.0 as string) = cast(gross as string))
UNION ALL
SELECT * FROM fill_non_chargebee
WHERE total_amount is not NULL
and cast(total_amount/100.0 * -1.0 as string) = cast(gross as string)

UNION ALL
SELECT * FROM fill_not_filled

union all
select * from fill_charge_not_filled
)



   select distinct * from result ;;
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

  dimension: product_4 {
    type: string
    sql: ${TABLE}.product_4 ;;
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

  dimension: product_4_period {
    type: string
    sql: ${TABLE}.product_4_period ;;
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

  dimension: original_amount4 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.original_amount4/100.0 ;;
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
  dimension: discount_amount4 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.discount_amount4/100.0 ;;
  }

    dimension: tax_1 {
      type: number
      value_format_name: usd
      sql: ${TABLE}.tax_1/100.0 ;;
    }
  dimension: content_invoice_credits_applied {
    type: number
    sql: ${TABLE}.content_invoice_credits_applied ;;
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

  dimension: tax_4 {
    type: number
    value_format_name: usd
    sql: ${TABLE}.tax_4/100.0 ;;
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
  measure: total_charge {
    type: sum
    sql: ${TABLE}.stripe_remitted ;;
  }


  }
