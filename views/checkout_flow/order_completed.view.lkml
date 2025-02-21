view: order_completed {
  derived_table: {
    sql:
      select
        context_page_path
        ,context_ip
        ,timestamp
      from JavaScript_upentertainment_checkout.order_completed;;
  }


  dimension: date {
    type: date
    sql: ${TABLE}.timestamp ;;
  }

  dimension: context_page_path {
    type: string
    sql: ${TABLE}.context_page_path ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  measure: x_page_volumne_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Total Unique IP Count (Filtered)"
    filters: [context_page_path: "some_value"]
  }

}
