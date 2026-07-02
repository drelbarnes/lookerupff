view: social_daily_snapshot {
  label: "Social Daily Snapshot"

  # Latest row per reporting date × profile (backfill appends duplicate profile-days in raw Redshift).
  sql_table_name: (
    SELECT s.*
    FROM (
      SELECT
        inner_s.*,
        ROW_NUMBER() OVER (
          PARTITION BY inner_s.date::date, inner_s.profile_id
          ORDER BY
            COALESCE(
              NULLIF(TRIM(inner_s.payload_schema_version::varchar), '')::int,
              1
            ) DESC,
            inner_s.ingested_at::timestamp DESC
        ) AS _snapshot_row_rank
      FROM agorapulse_webhook.social_daily_snapshot AS inner_s
      WHERE inner_s.profile_id IS NOT NULL
    ) AS s
    WHERE s._snapshot_row_rank = 1
  ) ;;

  # Reporting day (UTC). Cast if your warehouse column is VARCHAR/TIMESTAMP.
  dimension_group: snapshot_date {
    label: "Snapshot date"
    type: time
    datatype: date
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.date ;;
  }

  dimension: brand {
    hidden: yes
    label: "Brand (warehouse raw)"
    type: string
    sql: ${TABLE}.brand ;;
    description: "Value as stored in Redshift. Use brand_canonical for filters and reporting."
  }

  dimension: brand_canonical {
    label: "Brand"
    type: string
    sql:
      CASE
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('ovation', 'ovation tv', 'ovationtv') THEN 'Ovation TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('aspire', 'aspire tv', 'aspiretv') THEN 'Aspire TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('upff', 'up faith & family', 'up faith and family') THEN 'UPFF'
        ELSE ${TABLE}.brand
      END ;;
    description: "Normalized brand for rollup. UPFF and UP Faith & Family warehouse spellings both map to UPFF. Ovation / Aspire aliases match doc 02 / PROFILE_MAP."
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
    description: "Sum of impressions at profile-day grain (Agorapulse viewsCount). See docs/06 and docs/07."
  }

  measure: organic_video_views {
    label: "Organic video views"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.organic_video_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN ${platform} = 'tiktok'    THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'youtube'   THEN COALESCE(${TABLE}.video_views_count, 0)
        ELSE 0
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware audience grain. FB: organic_video_views_count; IG: organic_views_count; TT/YT: views_count or video_views_count (paid=0). See docs/07 §11."
  }

  measure: paid_video_views {
    label: "Paid video views"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.paid_video_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.paid_views_count, 0)
        ELSE 0
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware audience grain. FB: paid_video_views_count; IG: paid_views_count; TT/YT: 0. See docs/07 §11."
  }

  measure: total_video_views {
    label: "Total video views"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.video_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'tiktok'    THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'youtube'   THEN COALESCE(${TABLE}.video_views_count, 0)
        ELSE 0
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware audience grain. FB/YT: video_views_count; IG/TT: views_count. Total = organic + paid per platform. See docs/07 §11."
  }

  measure: total_engagements {
    label: "Total engagements"
    type: sum
    sql: ${engagements} ;;
    value_format_name: decimal_0
    description: "Sum of engagements at profile-day grain (Agorapulse engagementCount). See docs/06 and docs/07."
  }

  measure: avg_engagement_rate {
    label: "Engagement rate"
    type: average
    sql: ${engagement_rate} ;;
    value_format_name: percent_2
    description: "Average of warehouse engagement_rate at profile-day grain (Agorapulse engagementRatePerView; see index.ts). Dashboard KPI uses this measure—simple mean across rows, not weighted by impressions. Compare weighted_engagement_rate for sum(engagements)/sum(impressions) (doc 07 §6)."
  }

  measure: weighted_engagement_rate {
    label: "Engagement rate (weighted)"
    type: number
    sql: 1.0 * SUM(${engagements}) / NULLIF(SUM(${impressions}), 0) ;;
    value_format_name: percent_2
    description: "Weighted ratio: total engagements ÷ total impressions (doc 07 §6 Option B). Differs from avg_engagement_rate (mean of Agorapulse engagement_rate per row). Use in Explore when you need impression-weighted engagement."
  }
}
