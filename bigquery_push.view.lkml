view: bigquery_push {
  derived_table: {
    sql:
select user_id,
       event,
       context_os_name,
       timestamp
from android.notification_prompt_shown
union all
select cast(user_id as string) as user_id,
       event,
       context_os_name,
       timestamp
from ios.notification_prompt_shown
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: context_os_name {
    type: string
    sql: ${TABLE}.context_os_name ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  set: detail {
    fields: [user_id, event, context_os_name, timestamp_time]
  }
}
