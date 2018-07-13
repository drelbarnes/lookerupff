view: upff_linear_ratings {
  derived_table: {
    sql: select 'Total Day' as daypart, 'HH' as demo, 57 as rating, -.26 as VS2Q18Target, .02 as VSYA2Q17
      union all
      select 'Prime' as daypart, 'HH' as demo, 75 as rating, -.18 as VS2Q18Target, 0 as VSYA2Q17
      union all
      select 'Total Day' as daypart, 'W 25-54' as demo, 19 as rating, -.34 as VS2Q18Target, -.05 as VSYA2Q17
      union all
      select 'Prime' as daypart, 'W 25-54' as demo, 26, -.26 as VS2Q18Target, .04 as VSYA2Q17
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: daypart {
    type: string
    sql: ${TABLE}.daypart ;;
  }

  dimension: demo {
    type: string
    sql: ${TABLE}.demo ;;
  }

  dimension: rating {
    type: number
    sql: ${TABLE}.rating ;;
  }

  dimension: vs2q18target {
    type: number
    sql: ${TABLE}.vs2q18target ;;
  }

  dimension: vsya2q17 {
    type: number
    sql: ${TABLE}.vsya2q17 ;;
  }

  measure: rating_ {
    type: sum
    sql: ${TABLE}.rating ;;
  }

  measure: vs2q18target_ {
    type: sum
    sql: ${TABLE}.vs2q18target ;;
    value_format_name: percent_0
  }

  measure: vsya2q17_ {
    type: sum
    sql: ${TABLE}.vsya2q17 ;;
    value_format_name: percent_0
  }

  set: detail {
    fields: [daypart, demo, rating, vs2q18target, vsya2q17]
  }
}
