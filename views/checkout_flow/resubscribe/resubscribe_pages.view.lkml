view: resubscribe_pages {
  derived_table: {
    sql:
      WITH welcome as(
      select
        'welcome' as context_page_path
        ,context_ip
        ,user_id as unique_id
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/resubscribe/upfaithandfamily/%'
      ),

      thank_you as (
      select
        'thank_you' as context_page_path
        ,context_ip
        ,user_id as unique_id
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/resubscribe_thank_you/upfaithandfamily%'

      ),

      confirmation as (
      select
        'confirmation' as context_page_path
        ,context_ip
        ,user_id as unique_id
        ,timestamp
      from javascript_upentertainment_checkout.pages where context_page_path like '%welcome/confirmation_resubscribe/upfaithandfamily%' and  (user_id in (select  unique_id from thank_you) or user_id in (select  unique_id from welcome))
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
        ,CASE
    WHEN POSITION('&' IN SUBSTRING(context_page_url FROM POSITION('?rid=' IN context_page_url) + 5)) > 0 THEN
      SUBSTRING(
        context_page_url,
        POSITION('?rid=' IN context_page_url) + 5,
        POSITION('&' IN SUBSTRING(context_page_url FROM POSITION('?rid=' IN context_page_url) + 5)) - 1
      )
    ELSE
      SUBSTRING(context_page_url FROM POSITION('?rid=' IN context_page_url) + 5)
  END AS context_ip
  ,user_id as unique_id
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
    sql: ${TABLE}.context_ip;;
    label: "Order Resubscribed Count"
    filters: [context_page_path: "resubscribed"]
  }

  measure: welcome_page_id_count {
    type: count_distinct
    sql:${TABLE}.unique_id;;
    filters:[context_page_path: "welcome"]
    label: "Resubscribe Page id Count"
  }


  measure: thankyou_page_id_count {
    type: count_distinct
    sql: ${TABLE}.unique_id ;;
    label: "Thank You Page id Count"
    filters: [context_page_path: "thank_you"]
  }

  measure: confirmation_page_id_count {
    type: count_distinct
    sql: ${TABLE}.unique_id ;;
    label: "Confirmation Page id Count"
    filters: [context_page_path: "confirmation"]
  }

  measure: resubscribed_page_id_count {
    type: count_distinct
    sql: ${TABLE}.unique_id ;;
    label: "Order Resubscribed id Count"
    filters: [context_page_path: "resubscribed"]
  }




}
