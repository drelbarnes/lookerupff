view: transactions {
  sql_table_name: customers.transactions ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: affiliate_amount {
    type: number
    sql: ${TABLE}.affiliate_amount ;;
  }

  dimension: affiliate_amount_cents {
    type: number
    sql: ${TABLE}.affiliate_amount_cents ;;
  }

  dimension: affiliate_names {
    type: string
    sql: ${TABLE}.affiliate_names ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  dimension: amount_cents {
    type: number
    sql: ${TABLE}.amount_cents ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: card_country_code {
    type: string
    sql: ${TABLE}.card_country_code ;;
  }

  dimension: card_postal_code {
    type: string
    sql: ${TABLE}.card_postal_code ;;
  }

  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: country_code {
    type: string
    sql: ${TABLE}.country_code ;;
  }

  dimension: coupon_code_id {
    type: string
    sql: ${TABLE}.coupon_code_id ;;
  }

  dimension: created_at {
    type: string
    sql: ${TABLE}.created_at ;;
  }

  dimension: gift_emails {
    type: string
    sql: ${TABLE}.gift_emails ;;
  }

  dimension: income_tax_withholding_amount {
    type: number
    sql: ${TABLE}.income_tax_withholding_amount ;;
  }

  dimension: income_tax_withholding_amount_cents {
    type: number
    sql: ${TABLE}.income_tax_withholding_amount_cents ;;
  }

  dimension: income_tax_withholding_type {
    type: string
    sql: ${TABLE}.income_tax_withholding_type ;;
  }

  dimension: invoice_month {
    type: number
    sql: ${TABLE}.invoice_month ;;
  }

  dimension: invoice_year {
    type: number
    sql: ${TABLE}.invoice_year ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: promotion_id {
    type: number
    sql: ${TABLE}.promotion_id ;;
  }

  dimension: purchase_type {
    type: string
    sql: ${TABLE}.purchase_type ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: refunded_at {
    type: string
    sql: ${TABLE}.refunded_at ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: site_id {
    type: number
    sql: ${TABLE}.site_id ;;
  }

  dimension: site_title {
    type: string
    sql: ${TABLE}.site_title ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: tax_amount {
    type: number
    sql: ${TABLE}.tax_amount ;;
  }

  dimension: tax_amount_cents {
    type: number
    sql: ${TABLE}.tax_amount_cents ;;
  }

  dimension: units {
    type: number
    sql: ${TABLE}.units ;;
  }

  dimension: updated_at {
    type: string
    sql: ${TABLE}.updated_at ;;
  }

  dimension: user_email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.user_email ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: user_name {
    type: string
    sql: ${TABLE}.user_name ;;
  }

  dimension: vhx_fee {
    type: number
    sql: ${TABLE}.vhx_fee ;;
  }

  dimension: vhx_fee_cents {
    type: number
    sql: ${TABLE}.vhx_fee_cents ;;
  }

  measure: count {
    type: count
    drill_fields: [id, user_name, product_name]
  }
}
