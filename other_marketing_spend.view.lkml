view: other_marketing_spend {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.http_api.other_marketing_spend`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    datatype: date
    sql: ${TABLE}.date ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  set: detail {
    fields: [date, spend, channel]
  }
}
