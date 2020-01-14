view: bigquery_vimeo_ott_customers_v2 {
  sql_table_name: customers.vimeo_ott_customers_v2 ;;

  dimension: _frequency {
    type: string
    sql: ${TABLE}._Frequency ;;
  }

  dimension: action_type {
    type: string
    sql: ${TABLE}.Action_Type ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.Customer_ID ;;
  }

  dimension: days_tenure {
    type: string
    sql: ${TABLE}.Days_Tenure ;;
  }

  dimension_group: event {
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
    sql: ${TABLE}.Event_Date ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.Event_Type ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.Platform ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}.Price ;;
  }

  dimension_group: ticket_created {
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
    sql: ${TABLE}.Ticket_Created ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
