# The name of this view in Looker is "Cine Romantico"
view: cine_romantico {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  derived_table: {
    sql: select
          mvpd
          , date(week_starting) as week_starting
          , date(week_ending) as week_ending
          , channel
          , title
          , play_count
          , total_time_watched
          , asset_duration
          , avg_play_time
          , avg_playthrough_rate
          , asset_id
          , cast(replace(converted_hours_watched, ',', '') as decimal(12,2)) as converted_hours_watched
          , year
          from customers.cine_romantico  ;;
  }
  # No primary key is defined for this view. In order to join this view in an Explore,
  # define primary_key: yes on a dimension that has no repeated values.

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "Asset Duration" in Explore.

  dimension: asset_duration {
    type: string
    sql: ${TABLE}.asset_duration ;;
  }

  dimension: asset_id {
    type: string
    sql: ${TABLE}.asset_id ;;
  }

  dimension: avg_play_time {
    type: string
    sql: ${TABLE}.avg_play_time ;;
  }

  dimension: avg_playthrough_rate {
    type: number
    sql: ${TABLE}.avg_playthrough_rate ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: converted_hours_watched {
    type: number
    sql: ${TABLE}.converted_hours_watched ;;
  }

  dimension: mvpd {
    type: string
    sql: CASE
      WHEN ${TABLE}.mvpd = 'XUMO' then 'Xumo'
      else ${TABLE}.mvpd
      end;;
  }

  dimension: play_count {
    type: number
    sql: ${TABLE}.play_count ;;
  }

  dimension: title {
    type: string
    sql: CASE
      WHEN UPPER(${TABLE}.title) = 'CANT BUY MY LOVE ME' then 'CANT BUY MY LOVE'
      WHEN UPPER(${TABLE}.title) = 'CAN''T BUY MY LOVE ME' then 'CANT BUY MY LOVE'
      WHEN UPPER(${TABLE}.title) = 'CAN''T BUY MY LOVE' then 'CANT BUY MY LOVE'
      WHEN UPPER(${TABLE}.title) = 'ADVANCE  & RETREAT' then 'ADVANCE AND RETREAT'
      WHEN UPPER(${TABLE}.title) = 'TOUCHED' then 'TOUCHED BY ROMANCE'
      WHEN UPPER(${TABLE}.title) = 'THE COOKIE MOBSTER' then 'COOKIE MOBSTER'
      WHEN UPPER(${TABLE}.title) = 'FAREWELL MR KRINGLE' then 'FAREWELL MR. KRINGLE'
      WHEN UPPER(${TABLE}.title) = 'FLOWER GIRL' then 'THE FLOWER GIRL'
      WHEN UPPER(${TABLE}.title) = 'CUPID,INC.' then 'CUPIDINC.'
      WHEN UPPER(${TABLE}.title) = 'CUPIDINC' then 'CUPIDINC.'
      WHEN UPPER(${TABLE}.title) = 'CUPID' then 'CUPIDINC.'
      WHEN UPPER(${TABLE}.title) = 'THE MECHANICS OF LOVE' then 'MECHANICS OF LOVE'
      WHEN UPPER(${TABLE}.title) = 'MR FICTION' then 'MR. FICTION'
      WHEN UPPER(${TABLE}.title) = 'CAN''T BUY MY LOVE ME' then 'CANT BUY MY LOVE'
      WHEN UPPER(${TABLE}.title) = 'CAN''T BUY MY LOVE' then 'CANT BUY MY LOVE'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S ABIDING JOY' then 'LOVES ABIDING JOY'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S ENDURING PROMISE' then 'LOVES ENDURING PROMISE'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S EVERLASTING COURAGE' then 'LOVES EVERLASTING COURAGE'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S LONG JOURNEY' then 'LOVES LONG JOURNEY'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S UNENDING LEGACY' then 'LOVES UNENDING LEGACY'
      WHEN UPPER(${TABLE}.title) = 'LOVE''S UNFOLDING DREAM' then 'LOVES UNFOLDING DREAM'
      WHEN UPPER(${TABLE}.title) = 'A SON''S PROMISE' then 'A SONS PROMISE'
      WHEN UPPER(${TABLE}.title) = 'FIELDER''S CHOICE' then 'FIELDERS CHOICE'
      WHEN UPPER(${TABLE}.title) = 'VICKERY''S WILD RIDE' then 'VICKERYS WILD RIDE'
      WHEN UPPER(${TABLE}.title) = 'YOU''VE GOT A FRIEND' then 'YOUVE GOT A FRIEND'
      WHEN UPPER(${TABLE}.title) = 'WHERE THERE''S A WILL' then 'WHERE THERE IS A WILL'
      WHEN UPPER(${TABLE}.title) = 'WHERE THERES A WILL' then 'WHERE THERE IS A WILL'
      WHEN UPPER(${TABLE}.title) = 'OLIVER''S GHOST' then 'OLIVERS GHOST'
      WHEN UPPER(${TABLE}.title) = 'MR. WRITE' then 'MR WRITE'
      WHEN UPPER(${TABLE}.title) = 'READING, WRITING AND ROMANCE' then 'READING WRITING AND ROMANCE'
      WHEN UPPER(${TABLE}.title) = 'READING,WRITING AND ROMANCE' then 'READING WRITING AND ROMANCE'
      WHEN UPPER(${TABLE}.title) = 'CASA VITA' then 'LOVE THROWS A CURVE'
      else UPPER(${TABLE}.title)
      end;;
  }
  dimension: total_time_watched {
    type: string
    sql: ${TABLE}.total_time_watched ;;
  }

  dimension: week_ending {
    type: date
    sql: ${TABLE}.week_ending ;;
  }

  dimension: week_starting {
    type: date
    sql: ${TABLE}.week_starting ;;
  }

  dimension: year {
    type: string
    sql: ${TABLE}.year ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.


  measure: count {
    type: count
    drill_fields: []
  }
  measure: total_hrs {
    type: sum
    sql:  ${TABLE}.converted_hours_watched ;;
    }
    measure: total_plays {
      type: sum
      sql:  ${TABLE}.play_count ;;
  }
    measure: completion_rates {
    type: average
    value_format: "0.00%"
    sql:  CAST( ${TABLE}.avg_playthrough_rate   AS DOUBLE PRECISION) ;;
  }

  }
