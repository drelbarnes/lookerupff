view: churn_reasons_aggregated {
  sql_table_name: customers.churn_reasons_aggregated ;;

  dimension: high_price {
    type: number
    sql: ${TABLE}.high_price ;;
  }

  dimension: other {
    type: number
    sql: ${TABLE}.other ;;
  }

  dimension: save_money {
    type: string
    sql: ${TABLE}.save_money ;;
  }

  measure: high_price_total {
    type: sum
    sql: ${TABLE}.high_price ;;
  }

  measure: other_total {
    type: sum
    sql: ${TABLE}.other ;;
  }

  measure: save_money_total {
    type: sum
    sql: ${TABLE}.save_money ;;
  }
  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: vacation {
    type: number
    sql: ${TABLE}.vacation ;;
  }

  dimension: wait_content {
    type: number
    sql: ${TABLE}.wait_content ;;
  }

  measure: vacation_total {
    type: sum
    sql: ${TABLE}.vacation ;;
  }

  measure: wait_content_total {
    type: sum
    sql: ${TABLE}.wait_content ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }


}
