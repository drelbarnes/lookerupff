view: page_views_ip_date {
  derived_table: {
    sql: select id,context_ip, timestamp from javascript_upff_home.pages ;;
  }


  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }
  measure: views_count {
    type: count_distinct
    sql: ${TABLE}.id ;;
  }

  measure: unique_ip_views_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Total Unique IP Count"
  }
  set: detail {
    fields: [
      context_ip,
      timestamp_time
    ]
  }
}
