view: titles_id_mapping {
  sql_table_name: svod_titles.titles_id_mapping ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

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
    sql: ${TABLE}.date ;;
  }

  dimension: episode {
    type: number
    sql: ${TABLE}.episode ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
