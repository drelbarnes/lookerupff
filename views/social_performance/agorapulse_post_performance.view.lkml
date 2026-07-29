view: agorapulse_post_performance {
  label: "Social Post Last 30"

  # Segment event "Social Post Last 30" → social_post_last_30 (lean schema; replaces bloated social_post_snapshot).
  sql_table_name: agorapulse_webhook.social_post_last_30 ;;

  # Warehouse stores UTC (Agorapulse API). Convert for display/filters to match Agorapulse UI
  # (America/New_York). convert_tz: no avoids a second Looker-side conversion.
  dimension_group: publishing {
    label: "Publish date"
    type: time
    datatype: timestamp
    timeframes: [raw, time, date, week, month, quarter, year]
    convert_tz: no
    sql: CONVERT_TIMEZONE('UTC', 'America/New_York', ${TABLE}.publishing_date::timestamp) ;;
    description: "Publish time in America/New_York to align with Agorapulse UI. Warehouse column remains UTC."
  }

  dimension: brand {
    hidden: yes
    label: "Brand (warehouse raw)"
    type: string
    sql: ${TABLE}.brand ;;
    description: "Value as stored in Redshift. Use brand_canonical for filters aligned with Social Daily Snapshot."
  }

  dimension: brand_canonical {
    label: "Brand"
    type: string
    sql:
      CASE
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('ovation', 'ovation tv', 'ovationtv') THEN 'Ovation TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('aspire', 'aspire tv', 'aspiretv') THEN 'Aspire TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('upff', 'up faith & family', 'up faith and family') THEN 'UP Faith & Family'
        ELSE ${TABLE}.brand
      END ;;
    description: "Same normalization as social_daily_snapshot.brand_canonical (including UPFF + UP Faith & Family as one) so dashboard Brand filter matches both explores."
  }

  dimension: platform {
    label: "Platform"
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: post_id {
    label: "Post ID"
    type: string
    sql: ${TABLE}.post_id ;;
  }

  dimension: post_url {
    label: "Post URL"
    type: string
    sql: ${TABLE}.post_url ;;
  }

  dimension: event {
    label: "Event"
    type: string
    sql: ${TABLE}.event ;;
    description: "Segment event name (e.g. Social Post Last 30)."
  }

  measure: total_posts {
    label: "Total posts"
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.post_id IS NOT NULL AND ${TABLE}.post_id <> '' THEN ${TABLE}.post_id END) ;;
    value_format_name: decimal_0
    description: "Distinct posts for current filters. Multiple Segment rows per post (backfill + rolling windows) count once."
  }

  measure: post_impressions {
    label: "Post impressions (sum)"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.impressions_count, 0)
        WHEN ${platform} = 'tiktok'    THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'youtube'   THEN COALESCE(${TABLE}.video_views_count, 0)
        ELSE COALESCE(${TABLE}.impressions_count, ${TABLE}.views_count, 0)
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware post volume for top-post ranking. FB/TT: views_count (Agorapulse viewsCount); IG: impressions_count; YT: video_views_count. Meta deprecated FB impressions in favor of views."
  }

  measure: post_engagements {
    label: "Post engagements (sum)"
    type: sum
    sql: COALESCE(${TABLE}.engagement_count, 0) ;;
    value_format_name: decimal_0
    description: "Sum of engagement_count for rows in the query; context alongside impressions."
  }

  measure: post_video_views {
    label: "Post video views (sum)"
    type: sum
    sql:
      CASE
        WHEN ${platform} = 'facebook'  THEN COALESCE(${TABLE}.video_views_count, 0)
        WHEN ${platform} = 'instagram' THEN COALESCE(${TABLE}.organic_impressions_count, 0) + COALESCE(${TABLE}.paid_impressions_count, 0)
        WHEN ${platform} = 'tiktok'    THEN COALESCE(${TABLE}.views_count, 0)
        WHEN ${platform} = 'youtube'   THEN COALESCE(${TABLE}.video_views_count, 0)
        ELSE COALESCE(${TABLE}.video_views_count, 0)
      END ;;
    value_format_name: decimal_0
    description: "Platform-aware post video views. FB/YT: video_views_count; IG: organic_impressions_count + paid_impressions_count (Agorapulse content report; see IG video validation); TT: views_count."
  }
}
