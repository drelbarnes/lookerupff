view: config {
  derived_table: {
    sql: with a as (SELECT
                date('2026-04-30') as report_date
                )
          select * from a;;
}
 }
