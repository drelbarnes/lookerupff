view: campaign_conversion {
 derived_table: {
   sql:
  with marketing_page as(
  SELECT
    date(received_date) as report_date
    ,context_ip
    ,context_campaign_medium as campaign_medium
    ,context_campaign_name as campaign_name
  FROM javascript_upff_home.pages
  ),

  trial_created as (
  SELECT
    user_id
    ,context_ip
    ,date(timestamp) as report_date
  FROM javaScript_upentertainment_checkout.order_completed
  WHERE products_product_id like '%UP%'
  ),

  converted as (
  SELECT
    date(received_at) as report_date
    ,user_id
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items like '%UP%'
  )

  not_converted as (
  SELECT
      user_id
      ,DATE("timestamp") AS report_date
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancelled_at - content_subscription_trial_end) < 10000
      AND content_subscription_subscription_items LIKE '%UP%'
  ),

  join_data as (
  SELECT
    a.user_id
    ,a.context_ip
    ,a.report_date as trial_start_date
    ,CASE
      WHEN b.report_date is NOT NULL THEN 'Yes'
      WHEN c.report_date is NOT NULL THEN 'No'
      ELSE 'in trial'
    END AS has_converted
    FROM trial_created a
    LEFT JOIN converted b
    ON a.user_id = b.user_id
    LEFT JOIN not_converted c
    ON a.user_id = c.user_id
  )
  ;;
 }

 }
