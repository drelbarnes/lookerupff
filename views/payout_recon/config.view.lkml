view: config {
  derived_table: {
    sql: with a as (SELECT
                date('2026-07-31') as report_date
                )
          select * from a;;
}
 }
