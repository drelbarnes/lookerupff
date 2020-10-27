view: redshift_javascript_mybundle_tv {
  derived_table: {
    sql: SELECT p.status_date, o.anonymous_id, replace(regexp_substr(o.context_page_search,'\&(.*)'), '&mybundleid=', '') AS mybundle_id, o.user_email, p.topic FROM javascript.order_completed AS o, http_api.purchase_event AS p WHERE o.user_email = p.email AND o.context_page_search LIKE '%mybundleid%' ORDER BY p.status_date DESC
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: status_date {
    type: time
    sql: ${TABLE}.status_date ;;
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
    fields: [status_date_time, mybundle_id, user_email, topic]
  }
}
