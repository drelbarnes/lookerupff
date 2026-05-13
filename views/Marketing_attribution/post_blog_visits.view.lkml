view: post_blog_visits {
  derived_table: {
    sql:
      SELECT
        context_ip
        ,anonymous_id
        ,context_campaign_name as campaign_name
        ,context_campaign_source as source
        ,date(timestamp) as report_date
      FROM javascript.pages
      WHERE context_campaign_source = 'blog_upff'
      AND report_date >='2026-01-01'
       ;;

    sql_trigger_value: SELECT TO_CHAR( DATEADD(minute, -700, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month, quarter, year]
    datatype: date

    sql: ${TABLE}.report_date ;;
  }
  dimension: source {
    type: string
    sql:${TABLE}.source;;
  }

  dimension: campaign_name {
    label: "Campaign Name"
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  measure: total_visits {
    label: "Total Visits"
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    value_format_name: decimal_0
  }
}
