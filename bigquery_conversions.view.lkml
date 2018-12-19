view: bigquery_conversions {
  derived_table: {
    sql: select
      user_id,anonymous_id,
             received_at,
             "Web" as os
      from javascript.conversion
      union all
      select user_id, anonymous_id,
      received_at,
             "Android" as os
      from android.conversion
      union all
      select user_id, anonymous_id,
      received_at,
             "iOS" as os
      from ios.conversion
       ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  measure: count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  set: detail {
    fields: [anonymous_id, received_at_time, os]
  }
}
