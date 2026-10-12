view: config_backfill {
  derived_table:{
    sql: with a as (SELECT
                current_date - 1 as report_date
                )
          select * from a
    ;;
  }
}
