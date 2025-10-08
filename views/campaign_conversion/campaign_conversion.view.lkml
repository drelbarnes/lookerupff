view: campaign_conversion {
 derived_table: {
   sql:
  with marketing_page as(
  SELECT
    date(received_at) as report_date
    ,context_ip
    ,context_campaign_source as campaign_source
    ,context_campaign_name as campaign_name
    ,context_campaign_medium as campaign_medium
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
  ),

result as (
  SELECT
    jd.*
    ,mp.campaign_source
    ,mp.campaign_name
    ,mp.report_date as click_date
    ,campaign_medium
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
  ),

visit_count as (
  SELECT
    COUNT(distinct context_ip) as visit_count
    ,campaign_name
    ,campaign_source
    ,campaign_medium
  FROM marketing_page
  GROUP BY 2,3,4
),

trial_count as(
  SELECT
    COUNT(DISTINCT user_id) as trial_started_count
    ,campaign_source
    ,campaign_name
    ,campaign_medium
  FROM result
  GROUP BY 2,3,4
),

in_trial_count as (
  SELECT
    COUNT(DISTINCT user_id) as in_trial_count
    ,campaign_source
    ,campaign_name
    ,campaign_medium
  FROM result
  WHERE has_converted = 'in trial'
  GROUP BY 2,3,4

),

converted_count as (
SELECT
    COUNT(DISTINCT user_id) as converted_count
    ,campaign_source
    ,campaign_name
    ,campaign_medium
  FROM result

  WHERE has_converted = 'Yes'
  GROUP BY 2,3,4
)

SELECT
  vc.visit_count
  ,tc.trial_started_count
  ,itc.in_trial_count
  ,cc.converted_count
  ,vc.campaign_source
  ,vc.campaign_name
  ,vc.campaign_medium
FROM visit_count vc
LEFT JOIN trial_count tc
  ON vc.campaign_source = tc.campaign_source
 AND vc.campaign_name   = tc.campaign_name
 AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(tc.campaign_medium,'(none)')

LEFT JOIN in_trial_count itc
  ON vc.campaign_source = itc.campaign_source
 AND vc.campaign_name   = itc.campaign_name
 AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(itc.campaign_medium,'(none)')

LEFT JOIN converted_count cc
  ON vc.campaign_source = cc.campaign_source
 AND vc.campaign_name   = cc.campaign_name
 AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(cc.campaign_medium,'(none)')

  ;;
 }

  parameter: start_date {
    type: date
    default_value: "30 days ago"
  }
   dimension: campaign_source {
    type: string
    sql: ${TABLE}.campaign_source ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: campaign_medium {
    type: string
    sql: ${TABLE}.campaign_medium ;;
  }

  dimension: visit_count {
    type: number
    sql: ${TABLE}.visit_count ;;
  }

  dimension: trial_started_count {
    type: number
    sql: ${TABLE}.trial_started_count ;;
  }

  dimension: in_trial_count {
    type: number
    sql: ${TABLE}.in_trial_count ;;
  }

  dimension: converted_count {
    type: number
    sql: ${TABLE}.converted_count ;;
  }
  measure: total_visits {
    type: sum
    sql: ${visit_count} ;;
    value_format_name: decimal_0
  }

  measure: total_trials_started {
    type: sum
    sql: ${trial_started_count} ;;
    value_format_name: decimal_0
  }

  measure: total_in_trial {
    type: sum
    sql: ${in_trial_count} ;;
    value_format_name: decimal_0
  }

  measure: total_converted {
    type: sum
    sql: ${converted_count} ;;
    value_format_name: decimal_0
  }

 }
