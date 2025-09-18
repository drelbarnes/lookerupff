view: campaign_conversion {
 derived_table: {
   sql:
  with marketing_page as(
  SELECT
    date(received_at) as report_date
    ,context_ip
    ,context_campaign_source as campaign_source
    ,context_campaign_name as campaign_name
  FROM javascript_upff_home.pages
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}
  ),

  trial_created as (
  SELECT
    user_id
    ,context_ip
    ,date(timestamp) as report_date
  FROM javaScript_upentertainment_checkout.order_completed
  WHERE brand = 'upfaithandfamily'
  {% if start_date._parameter_value != "NULL" %}
    AND report_date >= {% parameter start_date %}
    {% endif %}
  ),

  converted as (
  SELECT
    date(received_at) as report_date
    ,user_id
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items like '%UP%'
  ),

  not_converted as (
  SELECT
      user_id
      ,DATE("timestamp") AS report_date
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
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

  SELECT
    jd.*
    ,mp.campaign_source
    ,mp.campaign_name
    ,mp.report_date as click_date
  FROM join_data jd
  LEFT JOIN marketing_page mp
  ON mp.context_ip = jd.context_ip
  and mp.report_date <= jd.trial_start_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY jd.context_ip
    ORDER BY
    CASE WHEN mp.campaign_name IS NULL THEN 1 ELSE 0 END,
    mp.report_date DESC
  ) = 1

  ;;
 }

  parameter: start_date {
    type: date
    default_value: "30 days ago"
  }
  dimension: trial_start_date {
    type: date
    sql: ${TABLE}.trial_start_date ;;
  }

  dimension: click_date {
    type: date
    sql: ${TABLE}.click_date ;;
  }


  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: has_converted {
    type: string
    sql: ${TABLE}.has_converted ;;
  }

  dimension: user_id{
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: campaign_source{
    type: string
    sql: ${TABLE}.campaign_source ;;
  }

  dimension: campaign_name{
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  measure: trial_start_count{
    type: count_distinct
    sql:${TABLE}.context_ip;;
  }

  measure: converted_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;
    filters:[has_converted: "Yes"]
  }





 }
