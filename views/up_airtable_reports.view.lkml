# The name of this view in Looker is "Up Airtable Reports"
view: up_airtable_reports {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: customers.up_airtable_reports ;;
  drill_fields: [tx_id]

  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  dimension: tx_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.tx_id ;;
  }
    # Here's what a typical dimension looks like in LookML.
    # A dimension is a groupable field that can be used to filter query results.
    # This dimension will be called "Alt Title Code" in Explore.

  dimension: alt_title_code {
    type: string
    sql: ${TABLE}.alt_title_code ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: contract {
    type: string
    sql: ${TABLE}.contract ;;
  }

  dimension: date {
    type: string
    sql: ${TABLE}.date ;;
  }

  dimension: duration {
    type: string
    sql: ${TABLE}.duration ;;
  }

  dimension: end_time {
    type: string
    sql: ${TABLE}.end_time ;;
  }

  dimension: product_code {
    type: string
    sql: ${TABLE}.product_code ;;
  }

  dimension: product_title {
    type: string
    sql: ${TABLE}.product_title ;;
  }

  dimension: program_id {
    type: number
    sql: ${TABLE}.program_id ;;
  }

  dimension: requires_special_attention {
    type: string
    sql: ${TABLE}.requires_special_attention ;;
  }

  dimension: series_title {
    type: string
    sql: ${TABLE}.series_title ;;
  }

  dimension: start_time {
    type: string
    sql: ${TABLE}.start_time ;;
  }

  dimension: tms_id {
    type: string
    sql: ${TABLE}.tms_id ;;
  }

  dimension: transmission_duration {
    type: string
    sql: ${TABLE}.transmission_duration ;;
  }
  measure: count {
    type: count
    drill_fields: [tx_id]
  }
}
