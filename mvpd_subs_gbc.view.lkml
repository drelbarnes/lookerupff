view: mvpd_subs_gbc {
  derived_table: {
    sql:
      select
        amazon,
        comcast,
        d2c,
        ifnull(dish_sling,0)+ifnull(cox,0)+ifnull(roku,0)+ifnull(appletv,0) as allothers,
        date(date) as date,
        year,
        month
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

  dimension: d2c {
    type: number
    sql: ${TABLE}.d2c ;;
  }

  dimension: all_others {
    type: number
    sql: ${TABLE}.allothers ;;
  }

  dimension: date {
    type: date
    sql: timestamp(${TABLE}.date) ;;
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

  measure: all_others_ {
    type: sum
    sql: ${TABLE}.allothers ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
