view: configg {
  derived_table:{
    sql: with a as (SELECT
           DATE '2025-07-01' as report_date)
          select * from a
    ;;
  }
  dimension: report_date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  }
