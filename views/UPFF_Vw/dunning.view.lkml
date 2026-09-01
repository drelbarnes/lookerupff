view: dunning {
derived_table: {
  sql:

    SELECT
      user_id
      ,CASE
        WHEN DATEDIFF(day,TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second',timestamp) = 0 THEN 'yes'
        ELSE 'no'
      END AS new_user
      ,date(timestamp) as report_date
      ,content_transaction_gateway as payment_gateway
    FROM chargebee_webhook_events.payment_failed
    where content_invoice_linked_payments_1_txn_date is null




  ;;
}

dimension: report_date {
  type: date
  sql: ${TABLE}.report_date ;;
}

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: payment_gateway {
    type: string
    sql: ${TABLE}.payment_gateway ;;
  }

dimension: new_user {
  type: string
  sql: ${TABLE}.new_user ;;
}



}
