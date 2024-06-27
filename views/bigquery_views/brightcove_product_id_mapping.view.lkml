view: brightcove_product_id_mapping {
  derived_table: {
    sql: with mapping as (
        select "27315" as product_id, "UP-Faith-Family-Monthly" as product_name,
        union all
        select "27315" as product_id, "UP-Faith-Family-Yearly" as product_name,
        union all
        select "137985" as product_id, "GaitherTV-USD-Monthly" as product_name,
        union all
        select "137985" as product_id, "GaitherTV-USD-Yearly" as product_name
      )
      select product_id, product_name from mapping ;;
  }

  dimension: product_id {
    description: "Vimeo OTT Product ID"
    type: string
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    description: "Chargebee Product Name"
    type: string
    sql: ${TABLE}.product_name ;;
  }
}
