view: converted {
  derived_table: {
    sql:
    with cfg AS (
    SELECT report_date
    FROM ${configg.SQL_TABLE_NAME}
),

    converted as (
      SELECT
        email
        ,subscription_frequency as billing_period
        ,'vimeo' as platform
        ,date(event_occurred_at) as report_date

      FROM customers.new_customers
      WHERE event_type = 'Free Trial to Paid'
      AND DATE(event_occurred_at) >= (SELECT MAX(report_date) FROM cfg)

      UNION ALL

      SELECT
        content_customer_email as email
        ,CASE
          WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
          ELSE 'yearly'
        END AS billing_period
        ,'web' as platform
        ,date(DATEADD(HOUR, 0, received_at)) as report_date

        FROM chargebee_webhook_events.subscription_activated
        WHERE content_subscription_subscription_items like '%UP%'
        and date(received_at) >= (SELECT MAX(report_date) FROM cfg)
         AND content_subscription_due_invoices_count = 0

),

dunning_converted  as (
 SELECT
        content_customer_email as email
        ,CASE
          WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
          ELSE 'yearly'
        END AS billing_period
        ,'web' as platform
        ,date(DATEADD(HOUR, 0, received_at)) as report_date

        FROM chargebee_webhook_events.subscription_activated
        WHERE content_subscription_subscription_items like '%UP%'
        and date(received_at) >= (SELECT MAX(report_date) FROM cfg)
         AND content_subscription_due_invoices_count != 0
),

dunning_paid as (

  SELECT
    content_customer_email::VARCHAR AS email,
    CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
    END AS billing_period
    ,'web' as platform
    ,DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second'- INTERVAL '5 hour') +8 AS report_date
  FROM chargebee_webhook_events.payment_succeeded
  WHERE content_subscription_subscription_items LIKE '%UP%'
    AND DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)
    AND (report_date::date - DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second')) <= 14
    AND content_invoice_dunning_attempts != '[]'),

dunning_cancelled as
(
      SELECT
      content_customer_email::VARCHAR AS email,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform,
      DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second'- INTERVAL '5 hour') +8 AS report_date

      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancel_reason_code in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer') AND content_subscription_subscription_items LIKE '%UP%'
        AND date(timestamp) >= (SELECT MAX(report_date) FROM cfg)
      )),

      sub_cancelled as (
SELECT
        content_customer_email as email,
        CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
        END AS billing_period,
        'web' AS platform,
        DATE(TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second'- INTERVAL '5 hour') +8 AS report_date
        --

    FROM chargebee_webhook_events.subscription_cancelled
    WHERE
        content_subscription_activated_at is NULL
        AND content_subscription_subscription_items LIKE '%UP%'
        and DATE(received_at) >= (SELECT MAX(report_date) FROM cfg)),




result2 as (
      select
        *, 'converted' as types
      FROM converted

      UNION ALL

      select
        *, 'dunning_converted' as types
      FROM dunning_converted

      UNION ALL
      select
        *, 'dunning_paid' as types
      FROM dunning_paid

      UNION ALL
      select
        *, 'dunning_cancelled' as types
      FROM dunning_cancelled

      UNION ALL
      SELECT
        *, 'gained' as types
      FROM ${free_trials_historical.SQL_TABLE_NAME}

      UNION ALL
      SELECT
      *,'not_converted' as types
      FROM sub_cancelled
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

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: converted_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "converted"]
  }

  measure: dunninig_converted_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_converted"]
  }

  measure: dunning_paid_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_paid"]
  }

  measure: dunning_cancelled_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "dunning_cancelled"]
  }

  measure: not_converted_count{
    type: count_distinct
    sql: ${TABLE}.email ;;
    filters: [types: "not_converted"]
  }

  measure: trial_7_days_ago {
    type: count_distinct
    sql: ${TABLE}.email;;
    filters: [types: "gained"]
  }

}
