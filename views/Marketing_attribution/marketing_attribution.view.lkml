view: marketing_attribution {
  derived_table: {
    sql:
    ,marketing_page as(
      SELECT
        *
      FROM ${visits.SQL_TABLE_NAME}
      WHERE 1=1
        --and context_campaign_name LIKE '%blue_skies%'
        AND report_date >='2026-01-01'
    ),

  trial_created as (
  SELECT
  distinct
    user_id
    ,context_ip
    ,anonymous_id
    ,date(timestamp) as report_date
  FROM javaScript_upentertainment_checkout.order_completed
  WHERE brand = 'upfaithandfamily'

    AND report_date >= '2026-01-01'

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
  ),

  converted as (
  SELECT
    date(received_at) as report_date
    ,user_id
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items like '%UP%'

    AND DATE(received_at)>= '2026-01-01'

  ),

  not_converted as (
  SELECT
      user_id
      ,DATE(timestamp) AS report_date
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
      AND content_subscription_subscription_items LIKE '%UP%'

    AND DATE(timestamp)>= TIMESTAMP '2026-01-01'

  ),

  join_data as (
  SELECT
  distinct
    a.user_id
    ,a.context_ip
    ,a.anonymous_id
    ,a.report_date
    ,'free_trial' as event
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

  UNION ALL

  SELECT
    user_id
    ,context_ip
    ,anonymous_id
    ,report_date
    ,'reacquisition' as event
    ,NULL as has_converted
  FROM reacquisition
  ),

result as (
  SELECT
    jd.user_id
    ,jd.context_ip
    ,jd.anonymous_id
    ,jd.report_date
    ,jd.has_converted
    ,jd.event
    ,mp.campaign_source
    ,mp.campaign_name
    ,mp.report_date as click_date
    ,mp.campaign_medium
    ,mp.campaign_content
    ,mp.marketing_platform
  ,ROW_NUMBER() OVER (
      PARTITION BY jd.user_id, jd.report_date
      ORDER BY
      mp.report_date DESC )AS row_num
      FROM join_data jd
      LEFT JOIN marketing_page mp
      ON  (mp.context_ip = jd.context_ip
      OR mp.anonymous_id = jd.anonymous_id)
      AND mp.report_date <= jd.report_date
      AND DATEDIFF(DAY, mp.report_date, jd.report_date) <= 7
      ),
      result2 AS (
      SELECT *
      FROM result
      WHERE row_num = 1
      )

  SELECT
    user_id
    ,event
    ,has_converted
    ,context_ip
    ,anonymous_id
    ,campaign_source
    ,campaign_name
    ,campaign_medium
    ,campaign_content
    ,marketing_platform
    ,report_date
  FROM result2;;

    sql_trigger_value: SELECT TO_CHAR( DATEADD(minute, -300, GETDATE()), 'YYYY-MM-DD');;
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
    sql: ${TABLE}.marketing_platform ;;
  }

  dimension: event {
    label: "Event"
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: has_converted {
    label: "has_converted"
    type: string
    sql: ${TABLE}.has_converted ;;
  }

  dimension: campaign_medium {
    label: "Campaign Medium"
    type: string
    sql: ${TABLE}.campaign_medium ;;
  }

  dimension: campaign_source_orig {
    label: "Campaign Source Orig"
    type: string
    sql: ${TABLE}.campaign_source ;;
  }

  dimension: campaign_content {
    label: "Campaign Content"
    type: string
    sql: ${TABLE}.campaign_content ;;
  }

  dimension: campaign_source {
    type: string
    sql: ${TABLE}.campaign_source;;
    }

  # ── Measures ─────────────────────────────────────────────────────────────

  measure: total_visits {
    label: "Total Visits"
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    value_format_name: decimal_0
  }

  measure: total_trials_started {
    label: "Total Trials Started"
    type: count_distinct
    filters: [event: "free_trial"]
    sql: ${TABLE}.user_id ;;
    value_format_name: decimal_0
  }


  measure: total_converted {
    label: "Total Converted"
    type: count_distinct
    filters: [has_converted: "Yes"]
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
