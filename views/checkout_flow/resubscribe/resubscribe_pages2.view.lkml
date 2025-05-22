view: resubscribe_pages2 {
  derived_table: {
    sql:
    WITH resubscribe_pages AS (
      SELECT *
      FROM ${resubscribe_pages.SQL_TABLE_NAME}
            ),

    resubscribe_pages2 as (
    SELECT
    DATE(resubscribe_pages.timestamp) AS date,
    COUNT(DISTINCT CASE
          WHEN resubscribe_pages.context_page_path = 'welcome'
          THEN resubscribe_pages.context_ip
          ELSE NULL
        END) AS welcome_page_count,
    COUNT(DISTINCT CASE
      WHEN resubscribe_pages.context_page_path = 'thank_you'
      THEN resubscribe_pages.context_ip
      ELSE NULL
    END) AS thankyou_page_count,
    COUNT(DISTINCT CASE
      WHEN resubscribe_pages.context_page_path = 'confirmation'
      THEN resubscribe_pages.context_ip
      ELSE NULL
    END) AS confirmation_page_count,
    COUNT(DISTINCT CASE
      WHEN resubscribe_pages.context_page_path = 'resubscribed'
      THEN resubscribe_pages.context_ip
      ELSE NULL
    END) AS resubscribed_page_count
FROM resubscribe_pages
WHERE resubscribe_pages.timestamp >= {% date_start filter_field %}
  --AND resubscribe_pages.timestamp <= {% date_end filter_field %}
GROUP BY 1),
result as(

      SELECT
      'Resubscribe Page Count' AS column_name,
      COALESCE(SUM(welcome_page_count), 0) AS value,
      1 AS page_order
      FROM resubscribe_pages2

      UNION ALL
      SELECT
      'Thank You Page Count' AS column_name,
      COALESCE(SUM(thankyou_page_count),0) AS value,
      2 as page_order
      FROM resubscribe_pages2
      UNION ALL

      SELECT
      'Confirmation Page Count' AS column_name,
      COALESCE(SUM(confirmation_page_count), 0) AS value,
      3 AS page_order
      FROM resubscribe_pages2

      UNION ALL
      SELECT
      'Order Resubscribed Count' AS column_name,
      COALESCE(SUM(resubscribed_page_count), 0) AS value,
      4 AS page_order
      FROM resubscribe_pages2)
      SELECT *
      from result
      ;;
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
