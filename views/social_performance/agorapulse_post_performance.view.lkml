view: agorapulse_post_performance {
  label: "Social Post Snapshot"

  sql_table_name: agorapulse_webhook.social_post_snapshot ;;

  # Warehouse column is publishing_date on social_post_snapshot.
  dimension_group: publishing {
    label: "Publish date"
    type: time
    datatype: timestamp
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.publishing_date ;;
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
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('upff', 'up faith & family', 'up faith and family') THEN 'UPFF'
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
    description: "Segment event name (e.g. Social Post Snapshot)."
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
    sql: COALESCE(${TABLE}.impressions_count, 0) ;;
    value_format_name: decimal_0
    description: "Sum of impressions_count for rows in the query; group by post_id for top-post ranking (see doc 07 §8)."
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
    sql: COALESCE(${TABLE}.video_views_count, 0) ;;
    value_format_name: decimal_0
    description: "Sum of video_views_count for rows in the query; context alongside impressions."
  }
}
