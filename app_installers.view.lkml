explore: app_installers {}
view: app_installers {
  derived_table: {
    sql: select anonymous_id, timestamp from android.app_installed
      union all
      select anonymous_id, timestamp from ios.app_installed
       ;;
  }

  measure: count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

# Added for AGM Tues-Mon weekly reporting
  dimension_group: week_start_tuesday {
    type: time
    timeframes: [raw, date, day_of_week, week, month]
    sql: CASE
      WHEN ${timestamp_day_of_week} = 'Tuesday' THEN ${timestamp_date}
      WHEN ${timestamp_day_of_week} = 'Wednesday' THEN dateadd(days, -1, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Thursday' THEN dateadd(days, -2, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Friday' THEN dateadd(days, -3,  ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Saturday' THEN dateadd(days, -4, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Sunday' THEN dateadd(days, -5, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Monday' THEN dateadd(days, -6, ${timestamp_date})
      END;;
    datatype: date
  }

  set: detail {
    fields: [anonymous_id, timestamp_time]
  }
}
