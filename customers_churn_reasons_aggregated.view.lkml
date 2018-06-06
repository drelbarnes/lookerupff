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
    drill_fields: [high_price,timestamp_date]
  }

  measure: other_total {
    type: sum
    sql: ${TABLE}.other ;;
    drill_fields: [other,timestamp_date]
  }

  measure: save_money_total {
    type: sum
    sql: ${TABLE}.save_money ;;
    drill_fields: [save_money,timestamp_date]
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
    drill_fields: [vacation,timestamp_date]
  }

  measure: wait_content_total {
    type: sum
    sql: ${TABLE}.wait_content ;;
    drill_fields: [wait_content,timestamp_date]
  }


  measure: count {
    type: count
    drill_fields: []
  }

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }


}
