view: checkout_pages2 {
  derived_table: {
    sql:
    WITH checkout_pages AS (
      SELECT *
      FROM ${checkout_pages.SQL_TABLE_NAME}
            ),
      checkout_pages2 as (
    SELECT
    DATE(checkout_pages.timestamp) AS date,
    {% if include_marketing_pages._parameter_value == "'yes'" %}
    COUNT(DISTINCT CASE
      WHEN (checkout_pages.context_page_path LIKE '%upfaithandfamily.com%' or checkout_pages.context_page_path LIKE '%upfaithandfamily.com/%')
            AND checkout_pages.data_table = 'marketing'
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS marketing_page_count,
  {% endif %}
    COUNT(DISTINCT CASE
          WHEN (checkout_pages.context_page_path IN ('/index.php/welcome/plans','/checkout/subscribe/purchase', '/'))
          THEN checkout_pages.context_ip
          ELSE NULL
        END) AS plans_page_count,
    COUNT(DISTINCT CASE
      WHEN ((checkout_pages.context_page_path LIKE '/index.php/welcome/create_account' OR checkout_pages.context_page_path LIKE '/create_account/') and data_table ='checkout_page')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS create_account_page_count,
    COUNT(DISTINCT CASE
      WHEN ((checkout_pages.context_page_path LIKE '/index.php/welcome/select_payment%' OR checkout_pages.context_page_path = '/payment')and data_table ='checkout_page')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS select_payment_page_count,
    COUNT(DISTINCT CASE
      WHEN ((checkout_pages.context_page_path = '/index.php/welcome/payment' or checkout_pages.context_page_path like '/index.php/welcome/confirm_payment/upfaithandfamily/%')and data_table ='checkout_page')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS payment_page_count,
    COUNT(DISTINCT CASE
      WHEN (checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell' OR checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell/upfaithandfamily/monthly' OR checkout_pages.context_page_path LIKE '/index.php/welcome/up_sell/upfaithandfamily/yearly' OR checkout_pages.context_page_path LIKE '/up_sell'OR checkout_pages.context_page_path =  '/checkout/subscribe' ) AND (checkout_pages.data_table  = 'order_completed')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS upsell_page_count,
    COUNT(DISTINCT CASE
      WHEN ((checkout_pages.context_page_path = '/index.php/welcome/confirmation' or checkout_pages.context_page_path = '/checkout/subscribe/receipt')and data_table ='checkout_page')
      THEN checkout_pages.context_ip
      ELSE NULL
    END) AS confirmation_page_count
FROM checkout_pages
WHERE checkout_pages.timestamp >= {% date_start filter_field %}
  AND checkout_pages.timestamp <= {% date_end filter_field %}
GROUP BY 1),
result as(

SELECT
    'Plans Page Count' AS column_name,
    COALESCE(SUM(plans_page_count), 0) AS value,
    1 AS page_order
FROM checkout_pages2

UNION ALL
{% if include_marketing_pages._parameter_value == "'yes'" %}
SELECT
    'Marketing Page Count' AS column_name,
    COALESCE(SUM(marketing_page_count),0) AS value,
    0 as page_order
    FROM checkout_pages2
    UNION ALL
 {% endif %}
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
    'UPSell Page/Order Completed Count' AS column_name,
    COALESCE(SUM(upsell_page_count), 0) AS value,
    5 AS page_order
FROM checkout_pages2

UNION ALL

SELECT
    'Confirmation Page Count' AS column_name,
    COALESCE(SUM(confirmation_page_count), 0) AS value,
    6 AS page_order
FROM checkout_pages2)
SELECT *
from result
;;
  }
  parameter: include_marketing_pages {
    type: string
    allowed_value: {
      label: "Yes"
      value: "yes"
    }
    allowed_value: {
      label: "No"
      value: "no"
    }
    default_value: "yes"
  }


  filter: filter_field {
    type: date
    label: "Start Date"
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


  measure: page_counts {
    type: sum
    sql: ${TABLE}.value ;;
  }
}
