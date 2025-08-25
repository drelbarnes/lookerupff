view: config {
  derived_table:{
    sql: SELECT
           '2025-06-01' as report_date
    ;;
  }
  dimension: report_date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  }
