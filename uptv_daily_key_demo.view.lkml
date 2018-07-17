view: uptv_daily_key_demo {
  derived_table: {
    sql: select 'Total Day' as daypart, 'HH' as demo, 52 as rating
      union all
      select 'Total Day' as daypart, 'Median Age' as demo, 53.5 as rating
      union all
      select 'Total Day' as daypart, 'W 18-49' as demo, 15 as rating
      union all
      select 'Total Day' as daypart, 'W 25-54' as demo, 16 as rating
      union all
      select 'Total Day' as daypart, 'P 18-49' as demo, 24 as rating
      union all
      select 'Total Day' as daypart, ' P 25-54' as demo, 26 as rating
      union all
      select 'Prime' as daypart, 'HH' as demo, 75 as rating
      union all
      select 'Prime' as daypart, 'Median Age' as demo, 58.1 as rating
      union all
      select 'Prime' as daypart, 'W 18-49' as demo, 21 as rating
      union all
      select 'Prime' as daypart, 'W 25-54' as demo, 20 as rating
      union all
      select 'Prime' as daypart, 'P 18-49' as demo, 27 as rating
      union all
      select 'Prime' as daypart, ' P 25-54' as demo, 26 as rating
       ;;
  }

  dimension: daypart{
  type: string
  sql: ${TABLE}.daypart ;;
  }

  dimension: demo {
    type:  string
    sql: ${TABLE}.demo ;;
  }

  measure: rating {
    type: sum
    sql: ${TABLE}.rating ;;
  }
  }
