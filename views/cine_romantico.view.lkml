# The name of this view in Looker is "Cine Romantico"
view: cine_romantico {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  derived_table: {
    sql: select
          mvpd
          , week_starting
          , week_ending
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
    sql: upper(${TABLE}.title) ;;
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
}
