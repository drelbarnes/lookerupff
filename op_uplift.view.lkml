view: op_uplift {
  sql_table_name: svod_titles.op_uplift ;;

  dimension: email {
    type: string
    sql: ${TABLE}.Email ;;
  }

  dimension_group: entry {
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
    sql: ${TABLE}.Entry_Date ;;
  }

measure: distinct_emails {
  type: count_distinct
  sql: ${email} ;;
}

  measure: count {
    type: count
    drill_fields: []
  }
}
