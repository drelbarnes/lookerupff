# The name of this view in Looker is "Cine Romantico"
view: cine_romantico {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: customers.cine_romantico ;;
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
    sql: ${TABLE}.mvpd ;;
  }

  dimension: play_count {
    type: number
    sql: ${TABLE}.play_count ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: total_time_watched {
    type: string
    sql: ${TABLE}.total_time_watched ;;
  }

  dimension: week_ending {
    type: string
    sql: ${TABLE}.week_ending ;;
  }

  dimension: week_starting {
    type: string
    sql: ${TABLE}.week_starting ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.

  measure: total_year {
    type: sum
    sql: ${year} ;;
  }

  measure: average_year {
    type: average
    sql: ${year} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_hrs {
    type: sum
    sql: ${converted_hours_watched} ;;

      }

  measure: play_rate {
    type: average
    sql: 100.0*${avg_playthrough_rate} ;;
  }

  measure: av_play_rate {
    type: average
    sql: ${avg_playthrough_rate} ;;
  }


  measure: play_count_final {
    type: sum
    sql: ${play_count} ;;
  }

}
