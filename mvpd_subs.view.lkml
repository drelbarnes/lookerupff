view: mvpd_subs {
  derived_table: {
    sql: select amazon, comcast, d2c, cox, "dish/sling" as dish, date(date) as date from svod_titles.mvpd_subs;;
    }

  dimension: amazon {
    type: number
    sql: ${TABLE}.amazon ;;
  }

  dimension: comcast {
    type: number
    sql: ${TABLE}.comcast ;;
  }

  dimension: d2c {
    type: number
    sql: ${TABLE}.d2c ;;
  }

  dimension: dish {
    type: number
    sql: ${TABLE}.dish ;;
  }

  dimension: cox {
    type: number
    sql: ${TABLE}.cox ;;
  }

  dimension: date {
    type: date_month
    sql: ${TABLE}.date ;;
  }

  measure: amazon_ {
    type: sum
    sql: ${TABLE}.amazon ;;
  }

  measure: comcast_ {
    type: sum
    sql: ${TABLE}.comcast ;;
  }

  measure: d2c_ {
    type: sum
    sql: ${TABLE}.d2c ;;
  }

  measure: dish_ {
    type: sum
    sql: ${TABLE}.dish ;;
  }

  measure: cox_ {
    type: sum
    sql: ${TABLE}.cox ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
