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

  # Reporting day (UTC/GMT). Keep convert_tz off so Looker does not shift to EST/EDT.
  dimension_group: snapshot_date {
    label: "Snapshot date"
    type: time
    datatype: date
    convert_tz: no
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
    # Hardcoded so the dashboard Brand filter does not SELECT DISTINCT over the
    # deduped snapshot subquery (that suggestion query spins and never returns).
    suggestions: [
      "Aspire TV",
      "Heartland on UP Faith & Family",
      "Ovation TV",
      "UP Faith & Family",
      "Uplift Someone",
      "UPtv"
    ]
    sql:
      CASE
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('ovation', 'ovation tv', 'ovationtv') THEN 'Ovation TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('aspire', 'aspire tv', 'aspiretv') THEN 'Aspire TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('upff', 'up faith & family', 'up faith and family') THEN 'UP Faith & Family'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('uptv', 'up tv') THEN 'UPtv'
        ELSE ${TABLE}.brand
      END ;;
    description: "Normalized brand for rollup. UPFF and UP Faith & Family warehouse spellings both map to UP Faith & Family. Ovation / Aspire aliases match doc 02 / PROFILE_MAP."
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
    sql: CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END ;;
    value_format_name: decimal_0
    description: "Sum of impressions at profile-day grain, excluding YouTube. Agorapulse does not provide YouTube impressions (the stored YouTube value is video views). YouTube rows contribute NULL."
  }

  # Single-value tile only. Charts keep total_impressions so a null YouTube series is not plotted as text.
  measure: total_impressions_display {
    label: "Total impressions"
    type: string
    sql:
      CASE
        WHEN COUNT(CASE WHEN LOWER(${platform}) <> 'youtube' THEN 1 END) = 0
         AND COUNT(CASE WHEN LOWER(${platform}) = 'youtube' THEN 1 END) > 0
        THEN 'N/A'
        ELSE TO_CHAR(
          SUM(CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END),
          'FM999,999,999,999,999'
        )
      END ;;
    description: "Total impressions for the KPI tile. N/A when the result is YouTube only; otherwise the non-YouTube sum. Blank when no rows match."
  }

  measure: total_impressions_rank {
    hidden: yes
    label: "Total impressions (rank)"
    type: number
    sql: COALESCE(SUM(CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END), -1) ;;
    description: "Sort key. YouTube impression totals are unavailable and rank last."
  }

  measure: organic_impressions {
    label: "Organic impressions"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN ${platform} = 'tiktok'    THEN COALESCE(${impressions}, 0)
        WHEN ${platform} = 'youtube'   THEN COALESCE(${impressions}, 0)
        ELSE 0
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware audience grain. FB/IG: organic_views_count; TT/YT: impressions (paid=0). Organic + paid = total_impressions where Agorapulse splits them."
  }

  measure: paid_impressions {
    label: "Paid impressions"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.paid_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.paid_views_count, 0)
        ELSE 0
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware audience grain. FB/IG: paid_views_count; TT/YT: 0. Organic + paid = total_impressions where Agorapulse splits them."
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
    sql:
      COALESCE(
        ${engagement_rate_per_view} / 100.0,
        1.0 * ${engagements} / NULLIF(${video_views}, 0)
      ) ;;
    value_format_name: percent_2
    description: "Mean of Agorapulse engagementRatePerView (percent divided by 100) for Facebook, Instagram, and TikTok. YouTube has no API rate, so those days use engagements divided by video views. Days with neither a rate nor video views are excluded."
  }

  measure: weighted_engagement_rate {
    label: "Engagement rate (weighted)"
    type: number
    sql: 1.0 * SUM(${engagements}) / NULLIF(SUM(CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END), 0) ;;
    value_format_name: percent_2
    description: "Weighted ratio: total engagements ÷ total non-YouTube impressions (doc 07 §6 Option B). YouTube impressions are excluded, so a YouTube-only result is null. Differs from avg_engagement_rate, which averages daily Agorapulse rates and uses engagements ÷ video views for YouTube."
  }
}
