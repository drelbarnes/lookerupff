view: chargebee_vimeo_ott_id_mapping {
  derived_table: {
    sql: SELECT a.customer_id, b.ott_user_id, b.product_id
        FROM customers.admin_tg_mw_6778_customers a
        left join customers.admin_tg_mw_6778_ott_users b
        on a.id = b.customer_id
      ;;
    datagroup_trigger: upff_daily_refresh_datagroup
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

  dimension: product_id {
    description: "OTT Product ID"
    type: number
    sql: ${TABLE}.product_id ;;
  }

}
