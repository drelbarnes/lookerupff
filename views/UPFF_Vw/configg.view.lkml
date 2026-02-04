view: configg {
  derived_table:{
    sql: with a as (SELECT
                DATEADD(month, -4, CURRENT_DATE) AS report_date)
          select * from a
    ;;
  }
  dimension: report_date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  }
