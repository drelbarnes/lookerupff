view: page_visits {
  derived_table: {
    sql:with result as(
      SELECT
          DATE(timestamp) AS day,
          COUNT(DISTINCT context_ip) AS unique_users
      FROM javascript_upff_home.pages
      WHERE EXTRACT(YEAR FROM timestamp) >= 2023
        AND ( {% condition url_filter %} path {% endcondition %} )
      GROUP BY DATE(timestamp))

      SELECT *
      FROM result;;
  }
  # Parameter for URL Filtering
  filter: url_filter {
    type: string
    suggestions: ["/stream/%", "/", "/sign-up/%", "/?%"]
    default_value: ""
  }



  dimension: time_period {
    type: date
    sql:${TABLE}.day;;
  }
  dimension: month_year {
    type: string
    sql: CAST(EXTRACT(YEAR FROM ${TABLE}.day) AS TEXT) || '-' || LPAD(CAST(EXTRACT(MONTH FROM ${TABLE}.day) AS TEXT), 2, '0') ;;
  }





  measure: unique_users {
    type: sum
    sql: ${TABLE}.unique_users ;;
  }

}
