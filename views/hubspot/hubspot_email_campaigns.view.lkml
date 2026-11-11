###############################################################################
# HubSpot Email Campaigns  (view 2 of 3)
# -----------------------------------------------------------------------------
# One row per campaign: lifetime results, HubSpot's own lifetime counters, and
# the average of ALL campaigns as a benchmark. Lets Campaign Lookup say "this
# campaign opened at 34% against a 28% benchmark" even when filtered to one
# campaign.
#
# Built for speed: a small pre-aggregated table copied to every Redshift node
# (DISTSTYLE ALL), built from the Email Activity table's 0/1 flags (plain SUMs).
# Looker only joins it when one of its fields is used.
#
# Benchmarks include only campaigns delivered to at least 100 people, so small
# test sends don't skew the average.
###############################################################################

view: hubspot_email_campaigns {
  label: "Campaign Benchmarks"

  derived_table: {
    datagroup_trigger: hubspot_segment_sync
    distribution_style: all
    sql:
      SELECT
          s.*,
          AVG(CASE WHEN s.recipients_delivered >= 100 THEN s.open_rate        END) OVER () AS benchmark_open_rate,
          AVG(CASE WHEN s.recipients_delivered >= 100 THEN s.click_rate       END) OVER () AS benchmark_click_rate,
          AVG(CASE WHEN s.recipients_delivered >= 100 THEN s.conversion_rate  END) OVER () AS benchmark_conversion_rate,
          AVG(CASE WHEN s.recipients_delivered >= 100 THEN s.unsubscribe_rate END) OVER () AS benchmark_unsubscribe_rate,
          AVG(CASE WHEN s.recipients_delivered >= 100 THEN s.spam_report_rate END) OVER () AS benchmark_spam_report_rate,
          SUM(CASE WHEN s.recipients_delivered >= 100 THEN 1 ELSE 0 END)           OVER () AS benchmark_campaign_count,
          PERCENT_RANK() OVER (ORDER BY COALESCE(s.open_rate, 0))                          AS open_rate_percentile,
          PERCENT_RANK() OVER (ORDER BY COALESCE(s.click_rate, 0))                         AS click_rate_percentile
      FROM (
          SELECT
              f.email_campaign_id,
              f.first_sent_at,
              f.last_sent_at,
              f.recipients_delivered,
              f.unique_opens,
              f.unique_clicks,
              f.conversions,
              f.unsubscribes,
              f.spam_reports,
              1.0 * f.unique_opens  / NULLIF(f.recipients_delivered, 0) AS open_rate,
              1.0 * f.unique_clicks / NULLIF(f.recipients_delivered, 0) AS click_rate,
              1.0 * f.conversions   / NULLIF(f.recipients_delivered, 0) AS conversion_rate,
              1.0 * f.unsubscribes  / NULLIF(f.recipients_delivered, 0) AS unsubscribe_rate,
              1.0 * f.spam_reports  / NULLIF(f.recipients_delivered, 0) AS spam_report_rate,
              ec.counters_sent      AS hubspot_reported_sent,
              ec.counters_delivered AS hubspot_reported_delivered,
              ec.counters_open      AS hubspot_reported_opens,
              ec.counters_click     AS hubspot_reported_clicks
          FROM (
              SELECT
                  email_campaign_id,
                  MIN(CASE WHEN is_sent = 1 THEN activity_at END) AS first_sent_at,
                  MAX(CASE WHEN is_sent = 1 THEN activity_at END) AS last_sent_at,
                  SUM(is_unique_delivered) AS recipients_delivered,
                  SUM(is_unique_open)      AS unique_opens,
                  SUM(is_unique_click)     AS unique_clicks,
                  SUM(is_conversion)       AS conversions,
                  SUM(is_unsubscribe)      AS unsubscribes,
                  SUM(is_spam_report)      AS spam_reports
              FROM ${hubspot_email_activity.SQL_TABLE_NAME}
              WHERE email_campaign_id IS NOT NULL
              GROUP BY 1
          ) f
          -- HubSpot's lifetime counters: complete even though Segment backfills only
          -- about a month of events. Remove any counters_* column your schema lacks.
          LEFT JOIN hubspot.email_campaigns ec
              ON ec.id::VARCHAR = f.email_campaign_id
      ) s
    ;;
  }

  dimension: email_campaign_id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.email_campaign_id ;;
  }

  dimension_group: first_sent {
    type: time
    label: "Campaign First Sent"
    description: "When the campaign sent its first email (US Eastern)."
    timeframes: [date, week, month, quarter, year]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.first_sent_at ;;
  }

  dimension_group: last_sent {
    type: time
    label: "Campaign Last Sent"
    description: "When the campaign most recently sent an email. Automated campaigns keep sending over time."
    timeframes: [date, week, month]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.last_sent_at ;;
  }

  dimension: lifetime_recipients_delivered {
    type: number
    label: "Lifetime Recipients Delivered"
    group_label: "Campaign Lifetime"
    description: "Everyone this campaign has ever been delivered to, across all dates."
    sql: ${TABLE}.recipients_delivered ;;
    value_format_name: decimal_0
  }

  dimension: lifetime_open_rate {
    type: number
    label: "Lifetime Open Rate"
    group_label: "Campaign Lifetime"
    description: "This campaign's open rate across all dates."
    sql: ${TABLE}.open_rate ;;
    value_format_name: percent_1
  }

  dimension: lifetime_click_rate {
    type: number
    label: "Lifetime Click Rate"
    group_label: "Campaign Lifetime"
    description: "This campaign's click rate across all dates."
    sql: ${TABLE}.click_rate ;;
    value_format_name: percent_2
  }

  dimension: open_rate_percentile {
    type: number
    label: "Open Rate Percentile"
    group_label: "Campaign Lifetime"
    description: "Where this campaign's open rate ranks among all campaigns: 90% means it beat 90% of campaigns."
    sql: ${TABLE}.open_rate_percentile ;;
    value_format_name: percent_0
  }

  dimension: benchmark_performance_tier {
    type: string
    label: "Open Rate vs Benchmark"
    group_label: "Campaign Lifetime"
    description: "Whether the campaign's lifetime open rate is above, near (within 2 points) or below the all-campaign benchmark."
    sql: CASE WHEN ${TABLE}.open_rate IS NULL THEN 'Not enough data'
              WHEN ${TABLE}.open_rate >= ${TABLE}.benchmark_open_rate + 0.02 THEN 'Above benchmark'
              WHEN ${TABLE}.open_rate <= ${TABLE}.benchmark_open_rate - 0.02 THEN 'Below benchmark'
              ELSE 'Near benchmark' END ;;
  }

  dimension: hubspot_reported_sent {
    type: number
    label: "HubSpot-Reported Sent (Lifetime)"
    group_label: "HubSpot Reported Totals"
    description: "Lifetime sends as reported by HubSpot. Use to check Emails Sent when event history is incomplete."
    sql: ${TABLE}.hubspot_reported_sent ;;
  }

  dimension: hubspot_reported_delivered {
    type: number
    label: "HubSpot-Reported Delivered (Lifetime)"
    group_label: "HubSpot Reported Totals"
    description: "Lifetime deliveries as reported by HubSpot."
    sql: ${TABLE}.hubspot_reported_delivered ;;
  }

  dimension: hubspot_reported_opens {
    type: number
    label: "HubSpot-Reported Opens (Lifetime)"
    group_label: "HubSpot Reported Totals"
    description: "Lifetime opens as reported by HubSpot."
    sql: ${TABLE}.hubspot_reported_opens ;;
  }

  dimension: hubspot_reported_clicks {
    type: number
    label: "HubSpot-Reported Clicks (Lifetime)"
    group_label: "HubSpot Reported Totals"
    description: "Lifetime clicks as reported by HubSpot."
    sql: ${TABLE}.hubspot_reported_clicks ;;
  }

  # Benchmarks are identical on every row, so MAX simply returns the value.
  measure: benchmark_open_rate {
    type: max
    label: "Benchmark Open Rate"
    group_label: "All-Campaign Benchmarks"
    description: "Average open rate across all campaigns delivered to at least 100 people. Compare a campaign's Open Rate against this."
    sql: ${TABLE}.benchmark_open_rate ;;
    value_format_name: percent_1
  }

  measure: benchmark_click_rate {
    type: max
    label: "Benchmark Click Rate"
    group_label: "All-Campaign Benchmarks"
    description: "Average click rate across all campaigns delivered to at least 100 people."
    sql: ${TABLE}.benchmark_click_rate ;;
    value_format_name: percent_2
  }

  measure: benchmark_conversion_rate {
    type: max
    label: "Benchmark Conversion Rate"
    group_label: "All-Campaign Benchmarks"
    description: "Average conversion rate across all campaigns delivered to at least 100 people."
    sql: ${TABLE}.benchmark_conversion_rate ;;
    value_format_name: percent_2
  }

  measure: benchmark_unsubscribe_rate {
    type: max
    label: "Benchmark Unsubscribe Rate"
    group_label: "All-Campaign Benchmarks"
    description: "Average unsubscribe rate across all campaigns delivered to at least 100 people."
    sql: ${TABLE}.benchmark_unsubscribe_rate ;;
    value_format_name: percent_2
  }

  measure: benchmark_spam_report_rate {
    type: max
    label: "Benchmark Spam Report Rate"
    group_label: "All-Campaign Benchmarks"
    description: "Average spam report rate across all campaigns delivered to at least 100 people."
    sql: ${TABLE}.benchmark_spam_report_rate ;;
    value_format_name: percent_3
  }

  measure: benchmark_campaign_count {
    type: max
    label: "Campaigns in Benchmark"
    group_label: "All-Campaign Benchmarks"
    description: "How many campaigns the benchmarks are averaged over."
    sql: ${TABLE}.benchmark_campaign_count ;;
  }

  measure: avg_open_rate_percentile {
    type: average
    label: "Open Rate Percentile"
    group_label: "All-Campaign Benchmarks"
    description: "Where the selected campaign ranks on open rate among all campaigns. 0.90 means better than 90% of campaigns."
    sql: ${TABLE}.open_rate_percentile ;;
    value_format_name: percent_0
  }
}
