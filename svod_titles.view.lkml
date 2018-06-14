view: titles {
  sql_table_name: svod_titles.svod_titles ;;

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: content_type {
    type: string
    sql: ${TABLE}.content_type ;;
  }

  dimension: franchise {
    type: string
    sql: ${TABLE}.franchise ;;
  }

  dimension: lf_sf {
    type: string
    sql: ${TABLE}.lf_sf ;;
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: studio {
    type: string
    sql: ${TABLE}.studio ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: up_title {
    type: string
    sql: ${TABLE}.up_title ;;
  }

  dimension: views {
    type: number
    sql: ${TABLE}.views ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_views {
    type: sum
    sql: ${views} ;;
  }

  measure: avg_views {
    type: average
    sql: ${views} ;;
  }

  measure: episode_count {
    type: count_distinct
    sql: ${up_title} ;;
  }
}
