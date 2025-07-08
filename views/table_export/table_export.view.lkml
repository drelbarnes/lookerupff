view: table_export {
  derived_table: {
    sql: select
    distinct customer_email
    ,customer_id
    from http_api.chargebee_subscriptions;;
   }
 dimension: customer_email {
   sql: ${TABLE}.customer_email ;;
 }
  dimension: customer_id {
    sql: ${TABLE}.customer_id ;;
  }
}
