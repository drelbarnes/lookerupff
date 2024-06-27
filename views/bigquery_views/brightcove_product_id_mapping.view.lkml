view: brightcove_product_id_mapping {
  derived_table: {
    sql:
      select "27315" as ProductId, "UP-Faith-Family-Monthly" as ProductName,
      union all
      select "27315" as ProductId, "UP-Faith-Family-Yearly" as ProductName,
      union all
      select "137985" as ProductId, "GaitherTV-USD-Monthly" as ProductName,
      union all
      select "137985" as ProductId, "GaitherTV-USD-Yearly" as ProductName,

  }

  dimension: ProductId {
    description: "Vimeo OTT Product ID"
    type: string
    sql: ${TABLE}.ProductId ;;
  }

  dimension: ProductName {
    description: "Chargebee Product Name"
    type: string
    sql: ${TABLE}.ProductName ;;
  }
}
