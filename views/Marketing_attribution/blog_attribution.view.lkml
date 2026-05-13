view: blog_attribution {
  derived_table: {
    sql: ,marketing_page_orig as(
      SELECT
        *
      FROM ${visits.SQL_TABLE_NAME}
      WHERE DATE(report_date) >='2026-01-01'
      and marketing_platform = 'HubSpot' and context_page_url like '%https://upfaithandfamily.com/blog%' and campaign_medium = 'email'),



       trial_created_oc as (
  SELECT
  distinct
    user_id
    ,context_ip
    ,anonymous_id
    ,date(timestamp) as report_date
  FROM javaScript_upentertainment_checkout.order_completed
  WHERE brand = 'upfaithandfamily'
    AND  DATE(report_date) >='2026-01-01'
  ),

trial_created_ou as (

  SELECT
  distinct
    user_id
    ,context_ip
    ,anonymous_id
    ,date(timestamp) as report_date
  FROM javascript_upentertainment_checkout.order_updated
  WHERE DATE(report_date) >='2026-01-01' and context_page_path like '%upfaith%'),

trial_created as (
select * from trial_created_oc
UNION ALL
select * from trial_created_ou where user_id not in (select user_id from trial_created_oc)
),
  reacquisition as (
  select
  distinct
    user_id
    ,context_ip
    ,anonymous_id
    ,date(timestamp) as report_date
  FROM javascript_upentertainment_checkout.order_resubscribed
  WHERE brand = 'upfaithandfamily'
  AND  DATE(report_date) >='2026-01-01'
  ),

join_data as (
SELECT *,'free_trial' as event FROM trial_created
UNION ALL
SELECT *,'reacquisition' as event FROM reacquisition
),

  final as (
  SELECT
    DISTINCT a.context_ip
    ,a.anonymous_id
    ,a.campaign_name
    ,b.event
    ,b.user_id
    ,a.report_date
  FROM marketing_page_orig a
  LEFT JOIN join_data b
  ON (a.anonymous_id = b.anonymous_id or a.context_ip = b.context_ip) and a.report_date <=b.report_date
  )

 select * from final;;
    sql_trigger_value: SELECT TO_CHAR( DATEADD(minute, -700, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]


  }

  dimension_group: report_date {
    type: time
    datatype: date

    timeframes: [date, week, month, quarter, year]
    sql: ${TABLE}.report_date ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: campaign_name {
    label: "Campaign Name"
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: marketing_platform {
    label: "Marketing Platform"
    type: string
    sql:
    CASE
      WHEN ${TABLE}.marketing_platform IS NULL
        OR ${TABLE}.marketing_platform IN ('Unknown', 'Others')
      THEN 'Others/Unknown'
      ELSE ${TABLE}.marketing_platform
    END ;;
  }

  dimension: event {
    label: "Event"
    type: string
    sql: ${TABLE}.event ;;
  }



  # ── Measures ─────────────────────────────────────────────────────────────


  measure: total_trials_started {
    label: "Total Trials Started"
    type: count_distinct
    filters: [event: "free_trial"]
    sql: ${TABLE}.user_id ;;
    value_format_name: decimal_0
  }


  measure: total_reacquisition {
    label: "Total Reacquisition"
    type: count_distinct
    filters: [event: "reacquisition"]
    sql: ${TABLE}.user_id ;;
    value_format_name: decimal_0
  }



}
