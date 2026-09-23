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
      ,CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
        ELSE 'yearly'
      END AS billing_frequency
      ,content_transaction_error_text as error_code
      ,'failed' as transaction_type
    FROM chargebee_webhook_events.payment_failed
    where content_invoice_linked_payments_1_txn_date is null
    and content_subscription_subscription_items_0_item_price_id like '%UP%'


    UNION ALL
    SELECT
    user_id
    ,'null' as new_user
    ,date(timestamp) as report_date
    ,content_customer_payment_method_gateway as payment_gateway
    ,CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
    END AS billing_frequency
  ,'NULL' as error_code
  ,'renewed' as transaction_type
    FROM chargebee_webhook_events.subscription_renewed






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

  dimension: billing_frequency{
    type: string
    sql: ${TABLE}.billing_frequency ;;
  }

  dimension: payment_gateway {
    type: string
    sql: ${TABLE}.payment_gateway ;;
  }

  dimension: error_code {
    type: string
    sql: ${TABLE}.error_code ;;
  }

  dimension: transaction_type {
    type: string
    sql: ${TABLE}.transaction_type ;;
  }


dimension: new_user {
  type: string
  sql: ${TABLE}.new_user ;;
}



}
