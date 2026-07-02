view: braintree_recon {
  derived_table: {
    sql: with paypal as (
          SELECT * from ${braintree.SQL_TABLE_NAME}),

      product1 as (
      SELECT
      customer_id
      ,email
      ,report_date
      ,payment_gateway
      ,payment_description
      ,product_1 as product
      ,product_1_period as period
      ,original_amount1 as original_amount
      ,discount_amount1 as discount_amount
      ,tax_1 as tax
      ,total_amount
      ,transaction_id
      ,ref_id
      ,original_amount1 - COALESCE(discount_amount1,0) as gross
      ,0.000015*(original_amount1 - COALESCE(discount_amount1,0) + tax_1) + ROUND(
    fee / NULLIF(
        (CASE WHEN product_1 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN product_2 IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN product_3 IS NOT NULL THEN 1 ELSE 0 END),
        0
    ),
    3
) AS fee
      FROM paypal

      ),
      product2 as (
      SELECT
      customer_id
      ,email
      ,report_date
      ,payment_gateway
      ,payment_description
      ,product_2 as product
      ,product_2_period as period
      ,original_amount2 as original_amount
      ,discount_amount2 as discount_amount
      ,tax_2 as tax
      ,total_amount
      ,transaction_id
      ,ref_id
      ,original_amount2 - COALESCE(discount_amount2,0)  as gross
      ,CASE
        WHEN product_2 is NULL THEN 0
        WHEN product_3 is NULL THEN 0.000015*(original_amount2 - COALESCE(discount_amount2,0) + tax_2) + fee/2.000
        ELSE fee/3.000
      END AS fee

      FROM paypal

      ),
      product3 as (
      SELECT
      customer_id
      ,email
      ,report_date
      ,payment_gateway
      ,payment_description
      ,product_3 as product
      ,product_3_period as period
      ,original_amount3 as original_amount
      ,discount_amount3 as discount_amount
      ,tax_3 as tax
      ,total_amount
      ,transaction_id
      ,ref_id
      ,original_amount3 - COALESCE(discount_amount3,0)  as gross
      ,CASE WHEN product_3 is not NULL THEN 0.000015*(original_amount3 - COALESCE(discount_amount3,0) + tax_3) + fee/3.000
      ELSE 0
      END as fee
      FROM paypal

      ),

      final as (
      select * from product1

      UNION ALL
      select * from product2

      UNION ALL
      select * from product3)

      select
      customer_id
      ,email
      ,report_date
      ,payment_gateway
      ,payment_description
      ,product
      ,period
      ,original_amount
      ,discount_amount
      ,CASE
      WHEN payment_description IN (
      'credit','charge_back'
      )
      THEN tax*-1
      ELSE tax
      end as tax
      ,total_amount
      ,transaction_id
      ,ref_id
      ,CASE
      WHEN payment_description IN (
      'credit','charge_back'
      )
      THEN
      CASE
      WHEN gross < 0 THEN gross
      ELSE gross * -1
      END
      ELSE gross
      END AS gross
      ,fee
      FROM final
      ;;
  }

  dimension: customer_id {
    primary_key: yes
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

  dimension: product {
    type: string
    sql: ${TABLE}.product ;;
  }

  dimension: period {
    type: string
    sql: ${TABLE}.period ;;
  }

  dimension: transaction_id {
    type: string
    sql: ${TABLE}.transaction_id ;;
  }

  dimension: ref_id {
    type: string
    sql: ${TABLE}.ref_id ;;
  }

  dimension: original_amount {
    type: number
    sql: ${TABLE}.original_amount ;;
  }

  dimension: discount_amount {
    type: number
    sql: ${TABLE}.discount_amount ;;
  }

  dimension: tax {
    type: number
    sql: ${TABLE}.tax/100.0 ;;
  }

  dimension: total_amount {
    type: number
    sql: ${TABLE}.total_amount ;;
  }

  dimension: gross {
    type: number
    sql: ${TABLE}.gross/100.0 ;;
  }

  dimension: fee {
    type: number
    sql:${TABLE}.fee ;;
    value_format_name: usd
  }


  measure: gaither_monthly_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "GaitherTV",
      period: "Monthly",
    ]

    value_format_name: usd
  }

  measure: gaither_yealy_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "GaitherTV",
      period: "Yearly"

    ]

    value_format_name: usd
  }



  measure: minno_monthly_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "Minno",
      period: "Monthly"

    ]

    value_format_name: usd
  }



  measure: minno_yealy_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "Minno",
      period: "Yearly"

    ]

    value_format_name: usd
  }



  measure: upff_monthly_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Monthly"

    ]

    value_format_name: usd
  }



  measure: upff_yealy_revenue {
    type: sum
    sql: ${gross} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Yearly"

    ]

    value_format_name: usd
  }

  measure: gaither_monthly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "GaitherTV",
      period: "Monthly"    ]

    value_format_name: usd
  }

  measure: gaither_yearly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "GaitherTV",
      period: "Yearly"    ]

    value_format_name: usd
  }




  measure: minno_monthly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "Minno",
      period: "Monthly"    ]

    value_format_name: usd
  }

  measure: minno_yearly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "Minno",
      period: "Yearly"    ]

    value_format_name: usd
  }




  measure: upff_monthly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Monthly"    ]

    value_format_name: usd
  }

  measure: upff_yearly_tax {
    type: sum
    sql: ${tax} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Yearly"    ]

    value_format_name: usd
  }

  measure: gaither_monthly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "GaitherTV",
      period: "Monthly"    ]

    value_format_name: usd
  }
  measure: gaither_yearly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "GaitherTV",
      period: "Yearly"
    ]

    value_format_name: usd
  }

  measure: minno_monthly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "Minno",
      period: "Monthly"
    ]

    value_format_name: usd
  }

  measure: minno_yearly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "Minno",
      period: "Yearly"
    ]

    value_format_name: usd
  }

  measure: upff_monthly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Monthly"
    ]

    value_format_name: usd
  }

  measure: upff_yearly_fee {
    type: sum
    sql: ${fee} ;;

    filters: [
      product: "UP-Faith-Family",
      period: "Yearly"
    ]

    value_format_name: usd
  }
}
