view: monthly_platform_user_count {
  derived_table: {
    sql: select user_id,platform,max(sent_at) as sent_at from http_api.purchase_event where topic="customer.product.renewed" group by 1,2
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

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  set: detail {
    fields: [user_id, platform, sent_at_time]
  }
}
