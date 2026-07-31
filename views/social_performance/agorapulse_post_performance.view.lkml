view: agorapulse_post_performance {
  label: "Social Post Snapshot"

  # Latest lifetime snapshot per post_id (backfill + rolling last_30 append duplicate rows).
  # Without this, SUM(impressions_count) multiplies lifetime metrics by ingest count and
  # scrambles Top 20 rankings vs Agorapulse. Canonical pattern: docs/07 §4 / docs/04 §3.3.
  sql_table_name: (
    SELECT s.*
    FROM (
      SELECT
        inner_s.*,
        ROW_NUMBER() OVER (
          PARTITION BY inner_s.post_id
          ORDER BY
            COALESCE(
              inner_s.ingested_at::timestamp,
              inner_s."timestamp"::timestamp
            ) DESC,
            inner_s."timestamp"::timestamp DESC
        ) AS _post_row_rank
      FROM agorapulse_webhook.social_post_snapshot AS inner_s
      WHERE inner_s.post_id IS NOT NULL
        AND TRIM(inner_s.post_id::varchar) <> ''
    ) AS s
    WHERE s._post_row_rank = 1
  ) ;;

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

  # Agorapulse ContentReportData.text (Segment → warehouse column "text").
  # Fallback to post_id when caption/title is blank (e.g. image-only posts).
  dimension: post_text {
    label: "Post title / text"
    type: string
    sql: COALESCE(NULLIF(TRIM(${TABLE}."text"), ''), ${TABLE}.post_id) ;;
    description: "Post caption/title from Agorapulse content report (text). Falls back to post_id when empty."
  }

  dimension: post_url {
    label: "Post URL"
    type: string
    sql: ${TABLE}.post_url ;;
    html:
      {% if value != blank %}
        <a href="{{ value }}" target="_blank" rel="noopener noreferrer">{{ value }}</a>
      {% else %}
        {{ rendered_value }}
      {% endif %} ;;
    description: "Native post permalink; opens in a new browser tab/window from Explore and dashboard grids."
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
    description: "Distinct posts for current filters. Explore is already latest-row-per-post; COUNT DISTINCT stays safe if filters expand the grain."
  }

  measure: post_impressions {
    label: "Post impressions"
    type: sum
    sql: COALESCE(${TABLE}.impressions_count, 0) ;;
    value_format_name: decimal_0
    description: "Latest-snapshot impressions_count per post (view deduped). Group by post_id for Top 20 ranking; sums across posts when rolled up (doc 07 §4 / §7)."
  }

  measure: post_engagements {
    label: "Post engagements"
    type: sum
    sql: COALESCE(${TABLE}.engagement_count, 0) ;;
    value_format_name: decimal_0
    description: "Latest-snapshot engagement_count per post (view deduped); context alongside impressions."
  }

  measure: post_video_views {
    label: "Post video views"
    type: sum
    sql: COALESCE(${TABLE}.video_views_count, 0) ;;
    value_format_name: decimal_0
    description: "Latest-snapshot video_views_count per post (view deduped); context alongside impressions."
  }
}
