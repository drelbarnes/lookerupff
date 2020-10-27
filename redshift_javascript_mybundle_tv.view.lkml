view: redshift_javascript_mybundle_tv {
  derived_table: {
    sql: SELECT p.received_at, o.anonymous_id, replace(regexp_substr(o.context_page_search,'\&(.*)'), '&mybundleid=', '') AS mybundle_id, o.user_email, p.topic FROM javascript.order_completed AS o, http_api.purchase_event AS p WHERE o.user_email = p.email AND o.context_page_search LIKE '%mybundleid%' ORDER BY p.received_at DESC
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: anonymous_id {
    type: string
    tags: ["anonmyous_id"]
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: mybundle_id {
    type: string
    sql: ${TABLE}.mybundle_id ;;
  }

  dimension: user_email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.user_email ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  set: detail {
    fields: [received_at_time, mybundle_id, user_email, topic]
  }
}
