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
      SELECT
          TO_CHAR(day, 'YYYY-MM-DD') AS day_format,
          TO_CHAR(day, 'YYYY-MM') AS month_format,
          SUM(daily_unique_ip) AS ip_sum
      FROM daily_distinct
      GROUP BY 1, 2 ;;
  }

  # Parameter for selecting time granularity
  parameter: time_granularity {
    type: string
    suggestions: ["day", "month"]
    default_value: "month"
  }

  # Dynamic time period based on user selection
  dimension: time_period {
    type: string
    sql:
      CASE
        WHEN {% parameter time_granularity %} = 'day' THEN ${TABLE}.day_format
        ELSE ${TABLE}.month_format
      END ;;
  }

  measure: ip_sum {
    type: sum
    sql: ${TABLE}.ip_sum ;;
  }
}
