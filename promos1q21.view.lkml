view: promos1q21 {
  derived_table: {
    sql: select * from svod_titles.promos1q21
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    datatype: date
    sql: ${TABLE}.Date ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.Collection ;;
  }

  set: detail {
    fields: [date, collection]
  }
}
