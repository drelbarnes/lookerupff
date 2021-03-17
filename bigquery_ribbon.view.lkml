view: bigquery_ribbon {
  derived_table: {
    sql: SELECT (mysql_upff_category_items_ingested_at_date) AS ingested_at,
       mysql_upff_category_items_name AS collection,
       mysql_upff_category_items_cat_name AS category_name,
       mysql_upff_category_items_cat_order AS cat_order,
       mysql_upff_category_items_item_order AS item_order
FROM looker.get_category_collections
order by 1,4,5
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: ingest_at {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.ingested_at ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: category_name {
    type: string
    sql: ${TABLE}.category_name ;;
  }

  dimension: cat_order {
    type: number
    sql: ${TABLE}.cat_order ;;
  }

  dimension: item_order {
    type: number
    sql: ${TABLE}.item_order ;;
  }

  set: detail {
    fields: [ingest_at_time, collection, category_name, cat_order, item_order]
  }
}
