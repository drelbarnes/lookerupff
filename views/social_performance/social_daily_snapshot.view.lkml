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

  dimension: engagement_rate_per_view {
    hidden: yes
    type: number
    sql: ${TABLE}.engagement_rate_per_view ;;
    description: "Raw Agorapulse engagementRatePerView, stored as a percent (2.56 means 2.56%). Null when the API omits the field."
  }

  measure: total_impressions {
    label: "Total impressions"
    type: sum
    sql: ${impressions} ;;
    value_format_name: decimal_0
    description: "Sum of impressions at profile-day grain (Agorapulse viewsCount). See docs/06 and docs/07."
  }

  measure: total_video_views {
    label: "Total video views"
    type: sum
    sql: ${video_views} ;;
    value_format_name: decimal_0
    description: "Sum of video_views at profile-day grain (Agorapulse videoViewsCount). Audience snapshot, not per-post video metrics. See docs/06 and docs/07 §5."
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
    sql:
      COALESCE(
        ${engagement_rate_per_view} / 100.0,
        1.0 * ${engagements} / NULLIF(${impressions}, 0)
      ) ;;
    value_format_name: percent_2
    description: "Mean of Agorapulse engagementRatePerView (percent divided by 100) at profile-day grain. Days with no API rate and no impressions are excluded. When the API rate is missing but impressions exist (YouTube), uses that day's engagements divided by impressions. Not the impression-weighted period ratio; see weighted_engagement_rate."
  }

  measure: weighted_engagement_rate {
    label: "Engagement rate (weighted)"
    type: number
    sql: 1.0 * SUM(${engagements}) / NULLIF(SUM(${impressions}), 0) ;;
    value_format_name: percent_2
    description: "Weighted ratio: total engagements ÷ total impressions (doc 07 §6 Option B). Differs from avg_engagement_rate, which averages daily rates and does not weight by impressions. Use in Explore when you need impression-weighted engagement."
  }
}
