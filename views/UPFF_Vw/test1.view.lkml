view: test1 {
  derived_table: {
    sql:
    select

      date(timestamp) as report_date
      ,content_customer_email as email
    from chargebee_webhook_events.subscription_cancelled
    where date(timestamp) > '2026-10-10'
    ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: report_date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  measure: email_count {
  type: count_distinct
  sql: ${TABLE}.email ;;
  }

}
