view: users {
  derived_table: {
    sql: select distinct user_id,
                created_at
from http_api.purchase_event
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

  dimension_group: created_at {
    type: time
    sql: ${TABLE}.created_at ;;
  }

  set: detail {
    fields: [user_id, created_at_time]
  }
}
