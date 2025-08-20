view: free_trials {
  derived_table: {
    sql:
    with chargebee as (
      SELECT
        content_subscription_id as user_id
        ,CASE
          WHEN content_subscription_billing_period_unit ='month' THEN 'monthly'
          ELSE 'yearly'
        END AS billing_period
        ,'web' as platform
        ,date(DATEADD(HOUR, -4, received_at)) as report_date
      FROM chargebee_webhook_events.subscription_created
      WHERE report_date >= '2025-06-01'
      AND content_subscription_subscription_items like '%UP%'
    ),

    vimeo as (
      SELECT distinct
        email
        ,date(event_occurred_at) as report_date
        ,subscription_frequency as billing_period
        FROM customers.new_customers
        WHERE event_type = 'New Free Trial' and report_date >='2025-06-01'
      ),
    vimeo2 as (
      SELECT
        a.email as user_id
        ,a.billing_period
        ,b.platform
        ,a.report_date
      FROM vimeo a
      LEFT JOIN (SELECT email, platform, date(timestamp) as report_date FROM vimeo_ott_webhook.customer_product_free_trial_created where report_date >= '2025-06-01') b
      ON a.email = b.email and a.report_date = b.report_date
      )
    SELECT * from vimeo2
    UNION ALL
    SELECT * from chargebee;;

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

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: billing_period{
    type: string
    sql: ${TABLE}.billing_period ;;
  }
}
