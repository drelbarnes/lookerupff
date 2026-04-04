view: social_daily_snapshot {
  label: "Social Daily Snapshot"

  sql_table_name: agorapulse_webhook.social_daily_snapshot ;;

  # Reporting day (UTC). Cast if your warehouse column is VARCHAR/TIMESTAMP.
  dimension_group: snapshot_date {
    label: "Snapshot date"
    type: time
    datatype: date
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.date ;;
  }

  dimension: brand {
    label: "Brand"
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: platform {
    label: "Platform"
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: profile_id {
    label: "Profile ID"
    type: string
    sql: ${TABLE}.profile_id ;;
  }

  dimension: profile_name {
    label: "Profile name"
    type: string
    sql: ${TABLE}.profile_name ;;
  }

  dimension: impressions {
    hidden: yes
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: video_views {
    hidden: yes
    type: number
    sql: ${TABLE}.video_views ;;
  }

  dimension: engagements {
    hidden: yes
    type: number
    sql: ${TABLE}.engagements ;;
  }

  dimension: engagement_rate {
    hidden: yes
    type: number
    sql: ${TABLE}.engagement_rate ;;
  }

  measure: total_impressions {
    label: "Total impressions"
    type: sum
    sql: ${impressions} ;;
    value_format_name: decimal_0
    description: "Sum of impressions at profile-day grain (Agorapulse viewsCount). See docs/05 and docs/06."
  }

  measure: total_video_views {
    label: "Total video views"
    type: sum
    sql: ${video_views} ;;
    value_format_name: decimal_0
    description: "Sum of video_views at profile-day grain (Agorapulse videoViewsCount). Audience snapshot, not per-post video metrics. See docs/05 and docs/06 §5."
  }

  measure: total_engagements {
    label: "Total engagements"
    type: sum
    sql: ${engagements} ;;
    value_format_name: decimal_0
    description: "Sum of engagements at profile-day grain (Agorapulse engagementCount). See docs/05 and docs/06."
  }

  measure: avg_engagement_rate {
    label: "Engagement rate"
    type: average
    sql: ${engagement_rate} ;;
    value_format_name: percent_2
    description: "Average of warehouse engagement_rate at profile-day grain (Agorapulse engagementRatePerView; see index.ts). Dashboard KPI uses this measure—simple mean across rows, not weighted by impressions. Compare weighted_engagement_rate for sum(engagements)/sum(impressions) (doc 06 §6)."
  }

  measure: weighted_engagement_rate {
    label: "Engagement rate (weighted)"
    type: number
    sql: 1.0 * SUM(${engagements}) / NULLIF(SUM(${impressions}), 0) ;;
    value_format_name: percent_2
    description: "Weighted ratio: total engagements ÷ total impressions (doc 06 §6 Option B). Differs from avg_engagement_rate (mean of Agorapulse engagement_rate per row). Use in Explore when you need impression-weighted engagement."
  }
}
