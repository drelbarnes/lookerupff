view: visit_to_trial {
  derived_table: {
    sql:
      ,marketing_page_orig as(
      SELECT
        *
      FROM ${visits.SQL_TABLE_NAME},

trials as (

SELECT
        *
      FROM ${marketing_attribution.SQL_TABLE_NAME}

)



;;
}

}
