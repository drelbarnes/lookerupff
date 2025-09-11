view: checkout_split {
  derived_table: {
    sql:

    WITH payment as(
      SELECT
      CASE
        WHEN context_page_path LIKE '/index.php/welcome/payment/%' THEN '/index.php/welcome/payment'
        WHEN context_page_path LIKE '/index.php/welcome/payment_condensed/%' THEN '/index.php/welcome/payment_condensed'
        ELSE context_page_path
      END AS context_page_path
        ,context_ip
        ,anonymous_id
        ,'checkout_page' as data_table
        ,date(timestamp) as report_date
      from javascript_upentertainment_checkout.pages
      WHERE context_page_path NOT LIKE '%gaither%'
      and (context_page_path like '/index.php/welcome/payment/%' or context_page_path like '/index.php/welcome/payment_condensed/%')
      and report_date >='2025-09-10'
),

order_completed as (

      select
      a.context_ip
      ,a.anonymous_id
      ,'order_completed' as data_table
      ,b.context_page_path
      ,a.brand
      ,a.timestamp
      from javaScript_upentertainment_checkout.order_completed a
      LEFT JOIN payment b
      on a.anonymous_id = b.anonymous_id and date(a.timestamp) = b.report_date
      ),

  order_completed2 as (
  SELECT
    CASE
      WHEN context_page_path = '/index.php/welcome/payment' THEN 'control'
      WHEN context_page_path = '/index.php/welcome/payment_condensed' THEN 'variant'
    ELSE context_page_path
    END as context_page_path
    ,context_ip
    ,anonymous_id
    ,data_table
    ,date(timestamp) as report_date
    FROM order_completed
    WHERE brand LIKE '%up%'
    and report_date >='2025-09-10'
  )

  SELECT
  *
  FROM payment

  UNION ALL

  SELECT
  *
  FROM order_completed2
      ;;
  }
  dimension: report_date {
    type: date
    sql: ${TABLE}.report_date ;;
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

  measure: control_traffic_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;
    filters:[context_page_path: "/index.php/welcome/payment"]
  }


  measure: variant_traffic_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    filters: [context_page_path: "/index.php/welcome/payment_condensed"]
  }

  measure: control_order_completed_count {
    type: count_distinct
    sql:${TABLE}.anonymous_id;;
    filters:[context_page_path: "control"]
  }


  measure: variant_order_completed_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    filters: [context_page_path: "variant"]
  }

  }
