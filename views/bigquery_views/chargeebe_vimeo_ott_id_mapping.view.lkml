view: chargebee_vimeo_ott_id_mapping {
  derived_table: {
    sql: SELECT a.customer_id, b.ott_user_id
        FROM customers.tg_middleware_abc9876_customers a
        left join customers.tg_middleware_abc9876_ott_users b
        on a.id = b.customer_id
      ;;
    interval_trigger: "15 minutes"
  }


  # Define your dimensions and measures here, like this:
  dimension: customer_id {
    description: "Chargebee Customer ID"
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: ott_user_id {
    description: "Vimeo OTT User ID"
    type: number
    sql: ${TABLE}.ott_user_id ;;
  }

}
