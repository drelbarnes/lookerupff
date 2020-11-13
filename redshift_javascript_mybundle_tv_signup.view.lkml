view: redshift_javascript_mybundle_tv_signup {
  derived_table: {
    sql: SELECT received_at, anonymous_id, replace(context_page_search, '?mybundleid=', '') AS mybundle_id FROM javascript_upff_home.pages WHERE context_page_search LIKE '%mybundleid%' ORDER BY received_at DESC
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
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: mybundle_id {
    type: string
    sql: ${TABLE}.mybundle_id ;;
  }

  set: detail {
    fields: [received_at_time, anonymous_id, mybundle_id]
  }
}
