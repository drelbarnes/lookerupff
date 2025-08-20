view: converted {
  derived_table: {
    sql:
    with converted as (
      SELECT
        email
        ,subscription_frequency as billing_period
        ,date(event_occurred_at) as report_date
      FROM customers.new_customers
      WHERE event_type = 'Free Trial to Paid' and report_date >='2025-06-01'

      UNION ALL

      SELECT
        content_customer_email as email
        ,CASE
          WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
          ELSE 'yearly'
        END AS billing_period
        ,date(DATEADD(HOUR, -4, received_at)) as report_date
        FROM chargebee_webhook_events.subscription_activated
        WHERE content_subscription_subscription_items like '%UP%' and date(received_at) >='2025-06-01'),

result2 as (
      select
        *, 'converted' as types
      FROM converted

      UNION ALL

      SELECT
        *, 'gained' as types
      FROM ${free_trials_historical.SQL_TABLE_NAME}
)
select * from result2
      ;;

  }
  dimension: date {
    type: date
    primary_key: yes
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time

    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: billing_period{
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: types{
    type: string
    sql: ${TABLE}.types ;;
  }

  measure: converted_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "converted"]
  }

  measure: trial_7_days_ago {
    type: count_distinct
    sql: ${TABLE}.email;;
    filters: [types: "gained"]
  }

}
