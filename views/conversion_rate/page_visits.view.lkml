view: page_visits {
  derived_table: {
    sql:
      SELECT
          DATE(timestamp) AS day,
          COUNT(DISTINCT user_id) AS unique_users
      FROM some_other_dataset
      WHERE EXTRACT(YEAR FROM timestamp) >= 2023
      {% if url_filter._is_filtered %}
        AND path IN ( {% condition url_filter %} path {% endcondition %} )
      {% endif %}
      GROUP BY DATE(timestamp) ;;
  }
  # Parameter for URL Filtering
  filter: url_filter {
    type: string
    suggestions: ["/stream/%", "/", "/sign-up/%", "/?%"]
    default_value: ""
  }

  dimension_group: day {
    type: time
    timeframes: [date, month, year]
    sql: ${TABLE}.day ;;
  }

  dimension: time_period {
    type: string
    sql:
    CASE
      WHEN {% parameter time_granularity %} = 'day' THEN TO_CHAR(${TABLE}.day, 'YYYY-MM-DD')
      ELSE TO_CHAR(${TABLE}.day, 'YYYY-MM')
    END ;;
  }


  measure: unique_users {
    type: sum
    sql: ${TABLE}.unique_users ;;
  }

}
