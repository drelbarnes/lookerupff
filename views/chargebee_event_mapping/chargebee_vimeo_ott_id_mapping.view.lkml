view: chargebee_vimeo_ott_id_mapping {
  derived_table: {
    sql: SELECT a.customer_id, b.ott_user_id
        FROM http_api.middleware_customer a
        left join http_api.middleware_ott_user b
        on a.id = b.customer_id
      ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
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
