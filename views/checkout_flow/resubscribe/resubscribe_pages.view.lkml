view: resubscribe_pages {
  derived_table: {
    sql:
      WITH result as(
      select
        CASE
          WHEN context_page_path like '%welcome/resubscribe/upfaithandfamily/%' THEN 'welcome'
          WHEN context_page_path like '%welcome/resubscribe_thank_you/upfaithandfamily%' THEN 'thank_you'
          WHEN context_page_path like '%welcome/confirmation_resubscribe/upfaithandfamily%' THEN 'confirmation'
          ELSE context_page_path
        END AS context_page_path
        ,CASE
          WHEN context_page_path = 'confirmation' THEN
            CASE
              WHEN POSITION('rid' IN context_page_url) > 0 THEN SUBSTRING(
        context_page_url,
        POSITION('rid' IN context_page_url),
        POSITION('&' IN SUBSTRING(context_page_url FROM POSITION('rid' IN context_page_url))) - 1
      )
            ELSE context_ip
          END
          ELSE context_ip
        END AS context_ip
        ,id
        ,timestamp
      from javascript_upentertainment_checkout.pages

      UNION ALL
      SELECT
      CASE
          WHEN context_page_path like '%/index.php/welcome/confirmation_resubscribe/upfaithandfamily/%'
          THEN 'resubscribed'
          ELSE context_page_path
          END AS context_page_path
        ,context_ip
        ,id
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

  measure: resubscribed_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Resubscribed Page Count"
    filters: [context_page_path: "resubscribed"]
  }


}
