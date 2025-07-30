view: subscriber_data {
  derived_table:{
    sql:
    WITH latest_failed AS (
  SELECT content_customer_email as email, MAX(timestamp) AS last_failed_time
  FROM chargebee_webhook_events.payment_failed
  WHERE content_invoice_dunning_status = 'in_progress'
    AND timestamp >= current_date - interval '13 day'  and content_subscription_subscription_items_0_item_price_id like '%UP%'
  GROUP BY email
),
latest_succeeded AS (
  SELECT content_customer_email as email, MAX(timestamp) AS last_success_time
  FROM chargebee_webhook_events.payment_succeeded
  WHERE timestamp >= current_date - interval '14 day'  and content_invoice_dunning_attempts not like '%attempt%'     AND content_customer_email IS NOT NULL and content_subscription_subscription_items_0_item_price_id like '%UP%'

  GROUP BY email
),
latest_failed_vimeo AS (
  SELECT email, MAX(timestamp) AS last_failed_time
  FROM vimeo_ott_webhook.customer_product_charge_failed
  WHERE  timestamp >= current_date - interval '13 day'
  GROUP BY email
),
latest_succeeded_vimeo AS (
  SELECT email, MAX(timestamp) AS last_success_time
  FROM  vimeo_ott_webhook.customer_product_renewed
  WHERE timestamp >= current_date - interval '14 day' AND email IS NOT NULL

  GROUP BY email
),
result as (
  SELECT
      customer_id as user_id
      ,customer_first_name as first_name
      ,customer_last_name as last_name
      ,customer_email as email
      ,customer_cs_marketing_opt_in as marketing_opt_in
      ,CASE
        WHEN subscription_status = 'in_trial' THEN 'free_trial'
        WHEN subscription_status = 'active' THEN 'enabled'
      ELSE subscription_status
      END AS subscription_status
      ,CASE
        WHEN subscription_billing_period_unit ='month' THEN 'monthly'
        ELSE 'yearly'
      END AS subscription_frequency
      ,subscription_subscription_items_0_item_price_id as plan_name
      ,'web' as platform
      ,customer_billing_address_zip as postal_code
      , date(timestamp 'epoch' + subscription_started_at * interval '1 second') as subscription_start_date
      ,date(timestamp 'epoch' + subscription_cancelled_at * interval '1 second') as subscription_end_date
      ,CASE
        WHEN subscription_status = 'active' and email in (select email from latest_failed) and email not in (select email from latest_succeeded) THEN 'Yes'
        ELSE 'No'
      END AS is_in_dunning
    FROM http_api.chargebee_subscriptions
    where subscription_subscription_items_0_item_price_id like 'UP%' and date(timestamp) = CURRENT_DATE

    UNION ALL
SELECT
 CAST(user_id AS VARCHAR) as user_id
,first_name
,last_name
,email
,marketing_opt_in
,status as subscription_status
,frequency as subscription_frequency
,CASE
  WHEN frequency = 'monthly' THEN 'UP-Faith-Family-Monthly'
  ELSE 'UP-Faith-Family-Yearly'
END AS plan_name
,platform
,NULL as postal_code
,DATE(TRY_CAST(TRIM(REPLACE(customer_created_at, 'UTC', '')) AS TIMESTAMP)) AS subscription_start_date,

CASE
  WHEN expiration_date IS NOT NULL
    AND TRIM(REPLACE(expiration_date, 'UTC', '')) <> ''
    THEN DATE(TRY_CAST(TRIM(REPLACE(expiration_date, 'UTC', '')) AS TIMESTAMP))
  ELSE NULL
END AS subscription_end_date


,CASE
  WHEN subscription_status = 'enabled' and email in (select email from latest_failed_vimeo) and email not in (select email from latest_succeeded_vimeo) THEN 'Yes'
  ELSE 'No'
  END AS is_in_dunning
from customers.all_customers
where action = 'subscription' and report_date = CURRENT_DATE),

hubspot AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY received_at DESC) AS row_num
    FROM hubspot.contacts
  ) sub
  WHERE row_num = 1
)

SELECT
a.*
,b.properties_topic_value as topic

FROM result a
LEFT JOIN hubspot b
ON LOWER(a.email) = b.email ;;
  }

  dimension: user_id {
    type:  string
    sql: ${TABLE}.user_id ;;
  }

  dimension: first_name {
    type:  string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type:  string
    sql: ${TABLE}.last_name ;;
  }

  dimension: email {
    type:  string
    sql: ${TABLE}.email;;
  }

  dimension: marketing_opt_in {
    type: yesno
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: plan_name {
    type: string
    sql: ${TABLE}.plan_name ;;
    label: "Brand"  # Optional alias
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: postal_code {
    type: string
    sql: ${TABLE}.postal_code ;;
  }

  dimension: subscription_start_date {
    type: date
    sql: ${TABLE}.subscription_start_date ;;
  }

  dimension: subscription_end_date {
    type: date
    sql: ${TABLE}.subscription_end_date ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  measure: total {
    type: count_distinct
    sql: ${TABLE}.user_id  ;;
  }


  }
