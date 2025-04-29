view: resubscribe_error {
  derived_table: {
    sql:

      select
        context_ip
        ,api_error_code
        ,timestamp
      from javascript_upentertainment_checkout.resubscribe_error ;;

      }

  dimension: date {
      type: date
      sql: ${TABLE}.timestamp ;;
  }


  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: api_error_code {
    type: string
    sql:  ${TABLE}.api_error_code ;;
  }

  measure: error_code_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;

    label: "API Error Code Count"
  }





}
