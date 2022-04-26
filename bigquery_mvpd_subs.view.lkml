view: bigquery_mvpd_subs {
  derived_table: {
    sql:
      select
        amazon,
        comcast,
        d2c as vimeo,
        ifnull(dish_sling,0)+ifnull(cox,0)+ifnull(roku,0)+ifnull(appletv,0) as allothers,
        date(date) as date,
        month,
        year
      from svod_titles.mvpd_subs;;
  }

  dimension: amazon {
    type: number
    sql: ${TABLE}.amazon ;;
  }

  dimension: comcast {
    type: number
    sql: ${TABLE}.comcast ;;
  }

  dimension: vimeo {
    type: number
    sql: ${TABLE}.vimeo ;;
  }

  dimension: all_others {
    type: number
    sql: ${TABLE}.allothers ;;
  }

  dimension: date {
    type: date
    sql: timestamp(${TABLE}.date) ;;
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
  }

  measure: amazon_ {
    type: sum
    sql: ${TABLE}.amazon ;;
  }

  measure: comcast_ {
    type: sum
    sql: ${TABLE}.comcast ;;
  }

  measure: vimeo_ {
    type: sum
    sql: ${TABLE}.vimeo ;;
  }

  measure: all_others_ {
    type: sum
    sql: ${TABLE}.allothers ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
