explore: app_installers {}
view: app_installers {
  derived_table: {
    sql: select anonymous_id, timestamp from android.application_installed
      union all
      select anonymous_id, timestamp from ios.application_installed
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

  set: detail {
    fields: [anonymous_id, timestamp_time]
  }
}
