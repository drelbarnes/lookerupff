view: customer_frequency {
  sql_table_name: `svod_titles.customer_frequency`
    ;;

  dimension_group: customer_created {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension_group: event_created {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.event_created_at ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${customer_id} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
