view: viewership {
  derived_table: {
    sql: select to_char(date(timestamp), 'day') as day, datepart('week',date(timestamp)) as Week, 'Web' as Platform, id from javascript.play
union all
select to_char(date(timestamp), 'day') as day, datepart('week',date(timestamp)) as Week, 'Android' as Platform, id from android.play
union all
select to_char(date(timestamp), 'day') as day, datepart('week',date(timestamp)) as Week, 'IOS' as Platform, id from ios.play ;;
  }

  dimension: day {
    type: string
    sql: ${TABLE}.day ;;
  }

  dimension: week {
    type: number
    sql: ${TABLE}.week ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: id{
    type: string
    sql: ${TABLE}.id ;;
  }

  measure: count {
    type: count_distinct
    sql: ${id} ;;
  }
  }
