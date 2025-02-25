view: trials_created {
  derived_table: {
    sql:
      WITH joined_data AS (
        SELECT context_ip, timestamp FROM javascript_upentertainment_checkout.order_completed
        UNION ALL
        SELECT context_ip, timestamp FROM javascript.order_completed
      ),
      daily_distinct AS (
        SELECT
          DATE(timestamp) AS day,
          COUNT(DISTINCT context_ip) AS daily_unique_ip
        FROM joined_data
        WHERE EXTRACT(YEAR FROM timestamp) >= 2023
        GROUP BY DATE(timestamp)
      )
      SELECT *
      from daily_distinct;;
  }



  dimension: day {
    type: date
    sql: ${TABLE}.day;;
  }
  dimension: month_year {
    type: string
    sql: CAST(EXTRACT(YEAR FROM ${TABLE}.timestamp) AS TEXT) || '-' || LPAD(CAST(EXTRACT(MONTH FROM ${TABLE}.timestamp) AS TEXT), 2, '0') ;;
  }



  measure: ip_sum {
    type: sum
    sql: ${TABLE}.daily_unique_ip ;;
  }
}
