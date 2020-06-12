view: redshift_data_warehouse_info {
  derived_table: {
    sql: select
          database,
          schema,
          "table",
          size,
          pct_used
        from
          svv_table_info
       ;;
  }

  measure: sum {
    type: sum
    sql: ${size} ;;
  }


  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: database {
    type: string
    sql: ${TABLE}.database ;;
  }

  dimension: schema {
    type: string
    sql: ${TABLE}.schema ;;
  }

  dimension: table {
    type: string
    sql: ${TABLE}."table" ;;
  }

  dimension: size {
    type: number
    sql: ${TABLE}.size ;;
  }

  dimension: pct_used {
    type: number
    sql: ${TABLE}.pct_used ;;
  }

  set: detail {
    fields: [database, schema, table, size, pct_used]
  }
}
