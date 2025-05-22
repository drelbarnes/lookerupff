view: resubscribe_pages {
  derived_table: {
    sql:
      WITH welcome as(
      select
        'welcome' as context_page_path
        ,context_ip
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/resubscribe/upfaithandfamily/%'
      ),

      thank_you as (
      select
        'thank_you' as context_page_path
        ,context_ip
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/resubscribe_thank_you/upfaithandfamily%' and context_ip in (select context_ip from welcome)

      ),

      confirmation as (
      select
        'confirmation' as context_page_path
        ,context_ip
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/confirmation_resubscribe/upfaithandfamily%' and context_ip in (select context_ip from thank_you)
      ),

      result as(
      select * from welcome

      UNION ALL

      select * from thank_you

      UNION ALL
      select * from confirmation

      UNION ALL
      SELECT
      CASE
          WHEN context_page_path like '%/index.php/welcome/confirmation_resubscribe/upfaithandfamily/%'
          THEN 'resubscribed'
          ELSE context_page_path
          END AS context_page_path
        ,context_ip
        ,timestamp
      FROM javascript_upentertainment_checkout.order_resubscribed)
      select * from result
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

  measure: welcome_page_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;
    filters:[context_page_path: "welcome"]
    label: "Resubscribe Page Count"
  }


  measure: thankyou_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Thank You Page Count"
    filters: [context_page_path: "thank_you"]
  }

  measure: confirmation_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Confirmation Page Count"
    filters: [context_page_path: "confirmation"]
  }

  measure: resubscribed_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Order Resubscribed Count"
    filters: [context_page_path: "resubscribed"]
  }


}
