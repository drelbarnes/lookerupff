view: page_views_ip_date {
  derived_table: {
    sql: select context_ip, timestamp from javascript_upff_home.pages ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  set: detail {
    fields: [
      context_ip,
      timestamp_time
    ]
  }
}
