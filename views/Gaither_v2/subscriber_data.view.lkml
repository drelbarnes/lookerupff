view: subscriber_data {
  derived_table:{
    sql:
    , latest_failed AS (
  SELECT content_customer_email as email, MAX(timestamp) AS last_failed_time
  FROM chargebee_webhook_events.payment_failed
  WHERE content_invoice_dunning_status = 'in_progress'
    AND timestamp >= current_date - interval '13 day'  and content_subscription_subscription_items_0_item_price_id like '%G%'
  GROUP BY email
),
latest_succeeded AS (
  SELECT content_customer_email as email, MAX(timestamp) AS last_success_time
  FROM chargebee_webhook_events.payment_succeeded
  WHERE timestamp >= current_date - interval '14 day'  and content_invoice_dunning_attempts not like '%attempt%'     AND content_customer_email IS NOT NULL and content_subscription_subscription_items_0_item_price_id like '%G%'

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
      vimeo_info as (
      SELECT DISTINCT
      CAST(user_id AS VARCHAR) as user_id
      ,email
      ,platform
      FROM customers.gaithertvplus_all_customers
      WHERE report_date = CURRENT_DATE
      and action = 'subscription'
      ),
      chargebee as(
      SELECT
      customer_id as user_id
      ,customer_first_name as first_name
      ,customer_last_name as last_name
      ,lower(customer_email)::VARCHAR as email
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
      ,customer_billing_address_state_code as state
      FROM http_api.chargebee_subscriptions
      where subscription_subscription_items_0_item_price_id like '%G%' and date(timestamp) = CURRENT_DATE
      ),

      web_user as (
      SELECT
      a.user_id as vimeo_id
      ,b.*
      FROM vimeo_info a
      LEFT JOIN chargebee b
      ON lower(a.email)= lower(b.email)
      WHERE a.platform = 'api'
      ),

      result as (
      SELECT
      *
      FROM web_user
      UNION ALL

      SELECT
      CAST(user_id AS VARCHAR) as vimeo_id
      ,CAST(user_id AS VARCHAR) as user_id
      ,first_name
      ,last_name
      ,email
      ,marketing_opt_in
      ,status as subscription_status
      ,frequency as subscription_frequency
      ,CASE
      WHEN frequency = 'monthly' THEN 'GaitherTV-USD-Monthly'
      ELSE 'GaitherTV-USD-Yearly'
      END AS plan_name
      ,CASE
      WHEN platform = 'web' THEN 'web(Vimeo)'
      ELSE platform
      END AS platform
      ,NULL as postal_code
      ,DATE(TRY_CAST(TRIM(REPLACE(customer_created_at, 'UTC', '')) AS TIMESTAMP)) AS subscription_start_date,

      CASE
      WHEN expiration_date IS NOT NULL
      AND TRIM(REPLACE(event_created_at, 'UTC', '')) <> '' and status in('cancelled','disabled','expired')
      THEN DATE(TRY_CAST(TRIM(REPLACE(event_created_at, 'UTC', '')) AS TIMESTAMP))
      ELSE NULL
      END AS subscription_end_date


      ,CASE
      WHEN subscription_status = 'enabled' and email in (select email from latest_failed_vimeo) and email not in (select email from latest_succeeded_vimeo) THEN 'Yes'
      ELSE 'No'
      END AS is_in_dunning
      ,state
      from customers.gaithertvplus_all_customers
      where action = 'subscription' and report_date = CURRENT_DATE and platform not in ('api')),



      vimeo_events as (
      SELECT
      *
      FROM  ${vimeo_webhook.SQL_TABLE_NAME}
      ),
      chargebee_events as (
      SELECT
      *
      FROM  ${chargebee_webhook.SQL_TABLE_NAME}

      ),
/*
      get_active_user as (
      SELECT distinct
      b.user_id
      ,b.email
      ,a.active_users_week

      FROM looker.bigquery_active_users a
      LEFT JOIN vimeo_info b
      ON CAST(a.user_id AS VARCHAR) = b.user_id
      ),
      */

      final as(
      SELECT DISTINCT
      a.*,
      ce.event AS topic,
      date(ce.timestamp) as report_date
      --CASE WHEN c.email IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_active_user
      FROM result a
      LEFT JOIN chargebee_events ce
      ON LOWER(TRIM(a.email)) = LOWER(TRIM(ce.email))
      /*
      LEFT JOIN get_active_user c
      ON LOWER(a.email) = LOWER(c.email)
      */
      WHERE a.platform in ('web')

      UNION ALL

      SELECT DISTINCT
      a.*,
      ve.event_text AS topic,
      date(ve.timestamp) as report_date
      --CASE WHEN c.email IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_active_user
      FROM result a
      LEFT JOIN vimeo_events ve
      ON a.user_id = ve.user_id
      /*
      LEFT JOIN get_active_user c
      ON LOWER(a.email) = LOWER(c.email)
      */
      WHERE a.platform not in ( 'api','web') or a.platform is NULL
      ),

      ranked_emails AS (
      SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY email ORDER BY subscription_start_date DESC) AS rn
      FROM final
      )
      SELECT *
      FROM ranked_emails
      WHERE rn = 1;;


    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -555, GETDATE()), 'YYYY-MM-DD');;
    distribution: "user_id"
    sortkeys: ["user_id"]
  }

  dimension: user_id {
    type:  string
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }

  dimension: first_name {
    type:  string
    sql: ${TABLE}.first_name ;;
  }

  dimension: report_date {
    type:  date
    sql: ${TABLE}.report_date ;;
  }

  dimension: last_name {
    type:  string
    sql: ${TABLE}.last_name ;;
  }

  dimension: state {
    type:  string
    sql: ${TABLE}.state ;;
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
