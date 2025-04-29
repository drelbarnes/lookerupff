view: resubscribe_pages {
  derived_table: {
    sql:

      select
        CASE
          WHEN context_page_path like '%welcome/resubscribe/%' THEN 'welcome'
          WHEN context_page_path like '%welcome/resubscribe_thank_you/upfaithandfamily%' THEN 'thank_you'
          WHEN context_page_path like '%confirmation_resubscribe/upfaithandfamily/%'
          THEN 'confirmation'
          ELSE context_page_path
        END AS context_page_path
        ,context_ip
        ,'checkout_page' as data_table
        ,id
        ,timestamp
      from javascript_upentertainment_checkout.pages;;

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
    label: "Welcome Page Count"
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


}
