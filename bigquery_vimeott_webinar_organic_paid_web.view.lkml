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

  dimension: date_formatted {
    sql: ${date_date} ;;
    html: {{ rendered_value | date: "%b %e" }} ;;
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


  measure: organic_num {
    type: sum
    sql: ${TABLE}.Orangic  ;;
  }

  measure: paid_num {
    type: sum
    sql: ${TABLE}.Paid ;;
  }

}
