view: checkout_pages2 {
  derived_table: {
    sql:
    WITH checkout_pages AS (select
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
      ),
      checkout_pages2 as (
    SELECT
    DATE(checkout_pages.timestamp) AS date,
    COUNT(DISTINCT CASE
        WHEN checkout_pages.context_page_path IN ('/index.php/welcome/plans', '/')
        THEN checkout_pages.context_ip
        ELSE NULL
      END) AS plans_page_count,
    COUNT(DISTINCT CASE
      WHEN checkout_pages.context_page_path LIKE '/index.php/welcome/create_account' OR checkout_pages.context_page_path LIKE '/create_account/'
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS create_account_page_count,
    COUNT(DISTINCT CASE
      WHEN checkout_pages.context_page_path LIKE '/index.php/welcome/select_payment' OR checkout_pages.context_page_path = '/payment'
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS select_payment_page_count,
    COUNT(DISTINCT CASE
      WHEN checkout_pages.context_page_path = '/index.php/welcome/payment'
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS payment_page_count,
    COUNT(DISTINCT CASE
      WHEN (checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell' OR checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell/upfaithandfamily/monthly' OR checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell/upfaithandfamily/yearly' OR checkout_pages.context_page_path LIKE '/up_sell') AND (checkout_pages.data_table  = 'order_completed')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS upsell_page_count,
    COUNT(DISTINCT CASE
      WHEN checkout_pages.context_page_path = '/index.php/welcome/confirmation'
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS confirmation_page_count
FROM checkout_pages
WHERE checkout_pages.timestamp >=
    DATEADD(day, -{% parameter in_last_n_days %} + 1, {% parameter current_date %})
    AND checkout_pages.timestamp <
    DATEADD(day, 1, {% parameter current_date %})
GROUP BY 1),
result as(
     SELECT
    'Plans Page Count' AS column_name,
    COALESCE(SUM(plans_page_count), 0) AS value,
    1 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Create Account Page Count' AS column_name,
    COALESCE(SUM(create_account_page_count), 0) AS value,
    2 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Select Payment Page Count' AS column_name,
    COALESCE(SUM(select_payment_page_count), 0) AS value,
    3 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Payment Page Count' AS column_name,
    COALESCE(SUM(payment_page_count), 0) AS value,
    4 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Upsell Page Count' AS column_name,
    COALESCE(SUM(upsell_page_count), 0) AS value,
    5 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Confirmation Page Count' AS column_name,
    COALESCE(SUM(confirmation_page_count), 0) AS value,
    6 AS page_order
FROM checkout_pages2)
SELECT *, ROUND(
    CAST((SELECT value FROM result WHERE page_order = 5) AS FLOAT) /
    CAST((SELECT value FROM result WHERE page_order = 1) AS FLOAT),
    2) AS conversion_rate
from result
;;
  }

  parameter: in_last_n_days{
    type : number
    default_value: "7"
    label: "Is in the last n days"

    }
  parameter: current_date {
    type: date
    default_value: "CURRENT_DATE"
    label: "Start Date"
  }


  dimension: date {
    type: date
    sql: ${TABLE}.date ;;
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

  dimension: plans_page_count {
    type: number
    sql: ${TABLE}.plans_page_count ;;
    label: "Plans Page Count"
    group_label: "Page Counts"
  }

  dimension: payment_page_count {
    type: number
    sql: ${TABLE}.payment_page_count ;;
    label: "Payment Page Count"
    group_label: "Page Counts"
  }

  dimension: create_account_page_count {
    type: number
    sql: ${TABLE}.create_account_page_count ;;
    label: "Create Account Page Count"
    group_label: "Page Counts"
  }
  dimension: select_payment_page_count {
    type: number
    sql: ${TABLE}.select_payment_page_count ;;
    label: "Select Payment Page Count"
    group_label: "Page Counts"
  }

  dimension: upsell_page_count {
    type: number
    sql: ${TABLE}.upsell_page_count ;;
    label: "Up Sell Page Count"
    group_label: "Page Counts"
  }

  dimension: confirmation_page_count {
    type: number
    sql: ${TABLE}.confirmation_page_count ;;
    label: "Confirmation Page Count"
    group_label: "Page Counts"
  }

  dimension: page_order {
    type: number
    sql:  ${TABLE}.page_order ;;
  }
  dimension: page_visit_counts {
    type:  number
    sql:  ${TABLE}.value ;;
    order_by_field: value_name
  }
  dimension: value_name {
    type:  string
    sql:  ${TABLE}.column_name ;;
  }

  dimension: conversion_rate {
    type: number
    sql: ${TABLE}.conversion_rate ;;
    value_format_name: "decimal_2"
  }

  measure: page_counts {
    type: sum
    sql: ${TABLE}.value ;;
  }
}
