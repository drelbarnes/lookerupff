view: agorapulse_post_performance {
  label: "Social Post Last 30"

  # Live Segment event "Social Post Last 30" → social_post_last_30 (not social_post_snapshot,
  # which stopped receiving rows ~2026-07-14 after the event name split).
  # Latest lifetime snapshot per post_id (rolling last_30 appends duplicate rows).
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
      FROM agorapulse_webhook.social_post_last_30 AS inner_s
      WHERE inner_s.post_id IS NOT NULL
        AND TRIM(inner_s.post_id::varchar) <> ''
    ) AS s
    WHERE s._post_row_rank = 1
  ) ;;

  # Warehouse stores UTC (Agorapulse API). Shift to EST/EDT for dashboard filters + display.
  # convert_tz: no avoids a second Looker-side conversion on top of CONVERT_TIMEZONE.
  dimension_group: publishing {
    label: "Publish date"
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: CONVERT_TIMEZONE('UTC', 'America/New_York', ${TABLE}.publishing_date::timestamp) ;;
    description: "Publish time in America/New_York (EST/EDT) to match Agorapulse UI. Warehouse column remains UTC."
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
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('uptv', 'up tv') THEN 'UPtv'
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
    description: "Segment event name (e.g. Social Post Last 30)."
  }

  measure: total_posts {
    label: "Total posts"
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.post_id IS NOT NULL AND ${TABLE}.post_id <> '' THEN ${TABLE}.post_id END) ;;
    value_format_name: decimal_0
    description: "Distinct posts for current filters. Explore is already latest-row-per-post; COUNT DISTINCT stays safe if filters expand the grain."
  }

  # Warehouse columns come from the raw content-report payload (Segment snake_case),
  # not from a single impressions_count alias:
  #   instagram → impressions_count (impressionsCount)
  #   facebook, tiktok → views_count (viewsCount). impressions_count is null on these rows.
  #   youtube → no impression field (video_views_count is views, shown separately)
  # GREATEST(impressions_count, video_views_count) ranked Facebook by video views and
  # scored TikTok and Facebook image posts at 0.
  dimension: post_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN -1
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.impressions_count, 0)
        WHEN LOWER(${platform}) IN ('facebook', 'tiktok') THEN COALESCE(${TABLE}.views_count, 0)
        ELSE COALESCE(${TABLE}.impressions_count, ${TABLE}.views_count, 0)
      END ;;
    description: "Sortable impression value. YouTube is -1 so it ranks last and the measure renders N/A."
  }

  measure: post_impressions {
    label: "Post impressions"
    type: max
    sql: ${post_impression_value} ;;
    value_format_name: decimal_0
    html: {% if value == nil or value < 0 %}N/A{% else %}{{ rendered_value }}{% endif %} ;;
    description: "Latest-snapshot impressions. Instagram uses impressions_count. Facebook and TikTok use views_count. YouTube has no Agorapulse impression field and shows N/A. Video views stay on Post video views."
  }

  measure: post_engagements {
    label: "Post engagements"
    type: max
    sql: COALESCE(${TABLE}.engagement_count, 0) ;;
    value_format_name: decimal_0
    description: "Latest-snapshot engagement_count per post (view deduped); context alongside impressions."
  }

  # Same platform mapping as social_daily_snapshot.total_video_views. Instagram post rows
  # carry no viewsCount, so impressionsCount stands in, matching the audience tile where
  # Instagram impressions and video views are the same field.
  measure: post_video_views {
    label: "Post video views"
    type: max
    sql:
      NULLIF(
        CASE
          WHEN LOWER(${platform}) IN ('facebook', 'youtube') THEN ${TABLE}.video_views_count
          WHEN LOWER(${platform}) = 'tiktok' THEN ${TABLE}.views_count
          WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.views_count, ${TABLE}.impressions_count)
          ELSE ${TABLE}.video_views_count
        END
      , 0) ;;
    value_format_name: decimal_0
    description: "Latest-snapshot video views per post, same fields as Total video views. Facebook and YouTube: video_views_count. TikTok: views_count. Instagram: views_count, else impressions_count. Null or zero renders as Looker's null symbol (photo posts have no video-view field)."
  }
}
