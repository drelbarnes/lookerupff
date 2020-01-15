view: bigquery_wicket_marketing_cost {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.customers.marketing_cost`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: start_date {
    type: date
    sql: ${TABLE}.start_date ;;
  }

  dimension: end_date {
    type: string
    sql: ${TABLE}.end_date ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: detail {
    type: string
    sql: ${TABLE}.detail ;;
  }

  dimension: string_field_4 {
    type: string
    sql: ${TABLE}.string_field_4 ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

  set: detail {
    fields: [
      start_date,
      end_date,
      source,
      detail,
      string_field_4,
      amount
    ]
  }
}
