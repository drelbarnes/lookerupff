view: uptv_daily_day_part {
  derived_table: {
    sql: select 'Total Day' as daypart, 'Median Age P2+' as demo, 53.5 as rating
      union all
      select 'Total Day' as daypart, 'W 25-54 (000)' as demo, 16 as rating
      union all
      select 'Daytime' as daypart, 'Median Age P2+' as demo, 49.6 as rating
      union all
      select 'Daytime' as daypart, 'W 25-54 (000)' as demo, 7 as rating
      union all
      select 'Fringe' as daypart, 'Median Age P2+' as demo, 50.3 as rating
      union all
      select 'Fringe' as daypart, 'W 25-54 (000)' as demo, 30 as rating
      union all
      select 'Prime' as daypart, 'Median Age P2+' as demo, 58.1 as rating
      union all
      select 'Prime' as daypart, 'W 25-54 (000)' as demo, 20 as rating
      union all
      select 'Late Night' as daypart, 'Median Age P2+' as demo, 52.9 as rating
      union all
      select 'Late Night' as daypart, 'W 25-54 (000)' as demo, 13 as rating
       ;;
  }

  dimension: day_part {
    type: string
    sql: ${TABLE}.daypart ;;
  }

  dimension: demo {
    type: string
    sql: ${TABLE}.demo ;;
  }

  measure: rating {
    type: sum
    sql: ${TABLE}.rating ;;
  }
  }
