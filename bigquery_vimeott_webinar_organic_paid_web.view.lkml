view: bigquery_vimeott_webinar_organic_paid_web {
  sql_table_name: vimeo_ott_webinar.organic_paid_web ;;

  dimension_group: date {
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
    sql: ${TABLE}.Date ;;
  }

  dimension: orangic {
    type: number
    sql: ${TABLE}.Orangic ;;
  }

  dimension: paid {
    type: number
    value_format_name: id
    sql: ${TABLE}.Paid ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
