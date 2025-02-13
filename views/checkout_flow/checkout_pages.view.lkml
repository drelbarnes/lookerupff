view: checkout_pages {
  derived_table: {
    sql:

      select
        context_page_path
        ,context_ip
        ,'checkout_page' as data_table
        ,timestamp
      from JavaScript_upentertainment_checkout.pages
      union all
      select
        context_page_path
        ,context_ip
        ,'order_completed' as data_table
        ,timestamp
      from JavaScript_upentertainment_checkout.order_completed
      ;;
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

  dimension: data_table {
    type: string
    sql: ${TABLE}.data_table ;;
  }

  measure: plans_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Plans Page Count"
    filters: [context_page_path:
      "/index.php/welcome/plans,
      /"]
  }

  measure: payment_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Payment Page Count"
    filters: [context_page_path: "/index.php/welcome/payment"]
  }

  measure: create_account_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Create Account Page Count"
    filters: [context_page_path:
      "/index.php/welcome/create_account,
      /create_account/"]
}
  measure: select_payment_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Select Payment Page Count"
    filters: [context_page_path:
      "/index.php/welcome/select_payment,
      /payment"]
  }

  measure: upsell_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Up Sell Page Count"
    filters: [ context_page_path:
      "/index.php/welcome/up_sell,
      /index.php/welcome/up_sell/upfaithandfamily/monthly,
      /index.php/welcome/up_sell/upfaithandfamily/yearly,
      /up_sell",
      data_table: "order_completed"]
  }

  measure: confirmation_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Confirmation Page Count"
    filters: [context_page_path: "/index.php/welcome/confirmation"]
  }



}
