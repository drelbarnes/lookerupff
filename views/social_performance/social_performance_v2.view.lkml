view: social_performance_v2 {
  label: "Social Performance"

  # ---------------------------------------------------------------------------
  # Merged replacement for agorapulse_post_performance + social_daily_snapshot.
  #
  # One table, two grains, tagged by record_type:
  #   'post'        → latest lifetime snapshot per post_id
  #                   (social_post_last_30; rolling last_30 appends duplicate rows)
  #   'profile_day' → latest row per UTC date × profile
  #                   (social_daily_snapshot; backfill appends duplicate profile-days)
  #
  # activity_date is the single date both grains are filtered on:
  #   post        → Eastern publish date (matches Agorapulse UI)
  #   profile_day → UTC reporting date
  # This keeps the dashboard's existing "date range means different things" semantics.
  #
  # Every measure is pinned to one record_type, so the grains never sum together.
  # Nested subqueries instead of a top-level WITH, so the PDT wrapper cannot collide.
  # ---------------------------------------------------------------------------
  derived_table: {
    datagroup_trigger: agorapulse_webhook_datagroup
    distribution_style: all
    sortkeys: ["activity_date"]
    sql:
      SELECT
        'post'::varchar(16)                                                          AS record_type,
        'post:' || p.post_id::varchar                                                AS row_key,
        p.post_id::varchar                                                           AS post_id,
        NULL::varchar(256)                                                           AS profile_id,
        NULL::varchar(256)                                                           AS profile_name,
        p.brand                                                                      AS brand,
        p.platform                                                                   AS platform,
        p."text"                                                                     AS post_text,
        p.post_url                                                                   AS post_url,
        p.event                                                                      AS event,
        CONVERT_TIMEZONE('UTC', 'America/New_York', p.publishing_date::timestamp)        AS publishing_ts_et,
        CONVERT_TIMEZONE('UTC', 'America/New_York', p.publishing_date::timestamp)::date  AS activity_date,
        NULL::bigint                                                                 AS impressions,
        p.impressions_count                                                          AS impressions_count,
        p.views_count                                                                AS views_count,
        p.organic_impressions_count                                                  AS organic_impressions_count,
        p.paid_impressions_count                                                     AS paid_impressions_count,
        p.organic_views_count                                                        AS organic_views_count,
        p.paid_views_count                                                           AS paid_views_count,
        NULL::bigint                                                                 AS organic_video_views_count,
        NULL::bigint                                                                 AS paid_video_views_count,
        p.video_views_count                                                          AS video_views_count,
        NULL::bigint                                                                 AS video_views,
        p.engagement_count                                                           AS engagements,
        NULL::double precision                                                       AS engagement_rate,
        NULL::double precision                                                       AS engagement_rate_per_view,
        NULL::bigint                                                                 AS followers,
        NULL::bigint                                                                 AS followers_gained
      FROM (
        SELECT
          inner_s.*,
          ROW_NUMBER() OVER (
            PARTITION BY inner_s.post_id
            ORDER BY
              COALESCE(inner_s.ingested_at::timestamp, inner_s."timestamp"::timestamp) DESC,
              inner_s."timestamp"::timestamp DESC
          ) AS _row_rank
        FROM agorapulse_webhook.social_post_last_30 AS inner_s
        WHERE inner_s.post_id IS NOT NULL
          AND TRIM(inner_s.post_id::varchar) <> ''
      ) AS p
      WHERE p._row_rank = 1

      UNION ALL

      SELECT
      'profile_day'::varchar(16)                                                   AS record_type,
      'profile_day:' || d.profile_id::varchar || ':' || d.date::date::varchar      AS row_key,
      NULL::varchar(256)                                                           AS post_id,
      d.profile_id::varchar                                                        AS profile_id,
      d.profile_name                                                               AS profile_name,
      d.brand                                                                      AS brand,
      d.platform                                                                   AS platform,
      NULL::varchar(65535)                                                         AS post_text,
      NULL::varchar(2048)                                                          AS post_url,
      NULL::varchar(256)                                                           AS event,
      NULL::timestamp                                                              AS publishing_ts_et,
      d.date::date                                                                 AS activity_date,
      d.impressions                                                                AS impressions,
      NULL::bigint                                                                 AS impressions_count,
      d.views_count                                                                AS views_count,
      NULL::bigint                                                                 AS organic_impressions_count,
      NULL::bigint                                                                 AS paid_impressions_count,
      d.organic_views_count                                                        AS organic_views_count,
      d.paid_views_count                                                           AS paid_views_count,
      d.organic_video_views_count                                                  AS organic_video_views_count,
      d.paid_video_views_count                                                     AS paid_video_views_count,
      d.video_views_count                                                          AS video_views_count,
      d.video_views                                                                AS video_views,
      d.engagements                                                                AS engagements,
      d.engagement_rate                                                            AS engagement_rate,
      d.engagement_rate_per_view                                                   AS engagement_rate_per_view,
      d.followers                                                                  AS followers,
      d.followers_gained                                                           AS followers_gained
      FROM (
      SELECT
      inner_s.*,
      ROW_NUMBER() OVER (
      PARTITION BY inner_s.date::date, inner_s.profile_id
      ORDER BY
      COALESCE(NULLIF(TRIM(inner_s.payload_schema_version::varchar), '')::int, 1) DESC,
      inner_s.ingested_at::timestamp DESC
      ) AS _row_rank
      FROM agorapulse_webhook.social_daily_snapshot AS inner_s
      WHERE inner_s.profile_id IS NOT NULL
      ) AS d
      WHERE d._row_rank = 1
      ;;
  }

  # ===========================================================================
  # KEYS
  # ===========================================================================

  dimension: row_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.row_key ;;
  }

  dimension: record_type {
    hidden: yes
    type: string
    sql: ${TABLE}.record_type ;;
    suggestions: ["post", "profile_day"]
    description: "post = one row per Agorapulse post. profile_day = one row per profile per UTC reporting day."
  }

  # ===========================================================================
  # PERIOD OVER PERIOD
  #   current_period → "Current Period" dashboard filter (any date range)
  #   prior_period   → "Prior Period" dashboard filter (any date range, set independently)
  #   period         → labels each row This period / Prior period (explore keeps only these rows)
  #   day_of_period  → 1..N within each window, for overlaid trend lines
  # Unsuffixed measures are the current period. *_prior and *_change are the comparison.
  # ===========================================================================

  filter: current_period {
    label: "Current Period"
    type: date
    description: "Current analysis window. Post metrics use the Eastern publish date; audience and engagement metrics use the UTC reporting date."
  }

  filter: prior_period {
    label: "Prior Period"
    type: date
    description: "Comparison window. Any range; it does not have to match the Current Period's length. If left open, it defaults to the same number of days immediately before the Current Period."
  }

  # date_end is exclusive. Fallbacks cover open-ended expressions ("after X", "before X").
  dimension: period_end {
    hidden: yes
    type: string
    sql: COALESCE({% date_end current_period %}::date, DATEADD(day, 1, CURRENT_DATE)::date) ;;
  }

  dimension: period_start {
    hidden: yes
    type: string
    sql: COALESCE({% date_start current_period %}::date, DATEADD(day, -30, ${period_end})::date) ;;
  }

  dimension: days_in_period {
    hidden: yes
    type: number
    sql: DATEDIFF(day, ${period_start}, ${period_end}) ;;
  }

  dimension: prior_period_end {
    hidden: yes
    type: string
    sql: COALESCE({% date_end prior_period %}::date, ${period_start}) ;;
  }

  dimension: prior_period_start {
    hidden: yes
    type: string
    sql: COALESCE({% date_start prior_period %}::date, DATEADD(day, -(${days_in_period})::int, ${prior_period_end})::date) ;;
  }

  dimension: period {
    label: "Period"
    type: string
    suggestions: ["This period", "Prior period"]
    sql:
      CASE
        WHEN ${activity_raw} >= ${period_start}       AND ${activity_raw} < ${period_end}       THEN 'This period'
        WHEN ${activity_raw} >= ${prior_period_start} AND ${activity_raw} < ${prior_period_end} THEN 'Prior period'
      END ;;
    description: "This period = inside Current Period. Prior period = inside Prior Period. If the two windows overlap, overlapping days count as This period."
  }

  dimension: day_of_period {
    label: "Day of period"
    type: number
    sql:
      DATEDIFF(
        day,
        CASE WHEN ${period} = 'This period' THEN ${period_start} ELSE ${prior_period_start} END,
        ${activity_raw}
      ) + 1 ;;
    description: "Day 1..N inside each window. Use as the x-axis with a measure and its _prior twin to overlay this period on the prior one."
  }

  dimension: current_daily_flag {
    hidden: yes
    type: number
    sql: CASE WHEN ${record_type} = 'profile_day' AND ${period} = 'This period' THEN 1 ELSE 0 END ;;
  }

  dimension: prior_daily_flag {
    hidden: yes
    type: number
    sql: CASE WHEN ${record_type} = 'profile_day' AND ${period} = 'Prior period' THEN 1 ELSE 0 END ;;
  }

  # ===========================================================================
  # DATES
  # ===========================================================================

  dimension_group: activity {
    label: "Activity"
    type: time
    datatype: date
    convert_tz: no
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.activity_date ;;
    description: "Shared date. Posts: Eastern publish date. Audience/engagement rows: UTC reporting date (replaces Snapshot date)."
  }

  # Posts only. convert_tz: no avoids a second Looker-side conversion on top of CONVERT_TIMEZONE.
  dimension_group: publishing {
    label: "Publish date"
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.publishing_ts_et ;;
    description: "Post publish time in America/New_York (EST/EDT) to match Agorapulse UI. Null on audience rows."
  }

  # ===========================================================================
  # SHARED DIMENSIONS (Brand / Platform filters)
  # ===========================================================================

  dimension: brand {
    hidden: yes
    label: "Brand (warehouse raw)"
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: brand_canonical {
    label: "Brand"
    type: string
    # Hardcoded so the filter does not SELECT DISTINCT over the derived table.
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
    description: "Normalized brand. UPFF and UP Faith & Family spellings both map to UP Faith & Family."
  }

  dimension: platform {
    label: "Platform"
    type: string
    suggestions: ["facebook", "instagram", "tiktok", "youtube"]
    sql: ${TABLE}.platform ;;
  }

  dimension: platform_reports_impressions {
    hidden: yes
    type: yesno
    sql: LOWER(${TABLE}.platform) <> 'youtube' ;;
    description: "Yes when the platform has an impression metric. YouTube is no."
  }

  dimension: platform_reports_video_views {
    hidden: yes
    type: yesno
    sql: LOWER(${TABLE}.platform) <> 'instagram' ;;
    description: "Yes when the platform is shown on the video views over time chart. Instagram is no."
  }

  dimension: brand_platform {
    label: "Brand · Platform"
    type: string
    sql: ${brand_canonical} || ' · ' || INITCAP(${platform}) ;;
  }

  # ===========================================================================
  # POST DIMENSIONS (null on audience rows)
  # ===========================================================================

  dimension: post_id {
    label: "Post ID"
    type: string
    sql: ${TABLE}.post_id ;;
  }

  dimension: post_text {
    label: "Post title / text"
    type: string
    sql: COALESCE(NULLIF(TRIM(${TABLE}.post_text), ''), ${TABLE}.post_id) ;;
    description: "Caption/title from the Agorapulse content report. Falls back to post_id when empty."
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
  }

  dimension: event {
    label: "Event"
    type: string
    sql: ${TABLE}.event ;;
  }

  #   instagram → impressions_count; facebook, tiktok → views_count; youtube → -1 (N/A)
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
  }

  dimension: post_organic_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN -1
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.organic_impressions_count, 0)
        WHEN LOWER(${platform}) = 'facebook' THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN LOWER(${platform}) = 'tiktok' THEN COALESCE(${TABLE}.views_count, 0)
        ELSE COALESCE(${TABLE}.organic_impressions_count, ${TABLE}.organic_views_count, 0)
      END ;;
  }

  dimension: post_paid_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN -1
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.paid_impressions_count, 0)
        WHEN LOWER(${platform}) = 'facebook' THEN COALESCE(${TABLE}.paid_views_count, 0)
        WHEN LOWER(${platform}) = 'tiktok' THEN 0
        ELSE COALESCE(${TABLE}.paid_impressions_count, ${TABLE}.paid_views_count, 0)
      END ;;
  }

  # ===========================================================================
  # PROFILE-DAY DIMENSIONS (null on post rows)
  # ===========================================================================

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

  dimension: engagement_rate_per_view {
    hidden: yes
    type: number
    sql: ${TABLE}.engagement_rate_per_view ;;
    description: "Raw Agorapulse engagementRatePerView, stored as a percent (2.56 means 2.56%)."
  }

  dimension: followers {
    hidden: yes
    type: number
    sql: ${TABLE}.followers ;;
  }

  dimension: followers_gained {
    hidden: yes
    type: number
    sql: ${TABLE}.followers_gained ;;
  }

  dimension: impression_value_excl_youtube {
    hidden: yes
    type: number
    sql: CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END ;;
  }

  dimension: organic_impression_value_excl_youtube {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN NULL
        WHEN ${platform} IN ('facebook', 'instagram') THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN ${platform} = 'tiktok' THEN COALESCE(${impressions}, 0)
        ELSE NULL
      END ;;
  }

  dimension: total_video_view_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN ${platform} IN ('facebook', 'youtube')  THEN COALESCE(${TABLE}.video_views_count, 0)
        WHEN ${platform} IN ('instagram', 'tiktok')  THEN COALESCE(${TABLE}.views_count, 0)
        ELSE 0
      END ;;
  }

  dimension: engagement_rate_value {
    hidden: yes
    type: number
    sql: COALESCE(${engagement_rate_per_view} / 100.0, 1.0 * ${engagements} / NULLIF(${video_views}, 0)) ;;
  }

  # ===========================================================================
  # POST MEASURES (current period)
  # ===========================================================================

  measure: total_posts {
    label: "Total posts"
    type: count_distinct
    sql: ${post_id} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "Distinct posts published in the Current Period (Eastern publish date)."
  }

  measure: total_posts_prior {
    label: "Total posts (prior)"
    type: count_distinct
    sql: ${post_id} ;;
    filters: [record_type: "post", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: total_posts_change {
    label: "Total posts (% change)"
    type: number
    sql: 1.0 * (${total_posts} - ${total_posts_prior}) / NULLIF(${total_posts_prior}, 0) ;;
    value_format_name: percent_1
  }

  measure: post_impressions {
    label: "Post impressions"
    type: max
    sql: ${post_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    html: {% if value == nil or value < 0 %}N/A{% else %}{{ rendered_value }}{% endif %} ;;
    description: "Latest-snapshot impressions. Instagram: impressions_count. Facebook/TikTok: views_count. YouTube: N/A."
  }

  measure: post_organic_impressions {
    label: "Organic impressions (post)"
    type: max
    sql: ${post_organic_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    html: {% if value == nil or value < 0 %}N/A{% else %}{{ rendered_value }}{% endif %} ;;
  }

  measure: post_paid_impressions {
    label: "Paid impressions (post)"
    type: max
    sql: ${post_paid_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    html: {% if value == nil or value < 0 %}N/A{% else %}{{ rendered_value }}{% endif %} ;;
  }

  measure: post_engagements {
    label: "Post engagements"
    type: max
    sql: COALESCE(${engagements}, 0) ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
  }

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
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
  }

  # ===========================================================================
  # AUDIENCE & ENGAGEMENT MEASURES (current period, with _prior / _change)
  # ===========================================================================

  # --- Followers -------------------------------------------------------------
  measure: followers_gained_total {
    label: "Followers gained"
    type: sum
    sql: ${followers_gained} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Gross followers/subscribers gained. Not net growth."
  }

  measure: followers_gained_total_prior {
    label: "Followers gained (prior)"
    type: sum
    sql: ${followers_gained} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: followers_gained_total_change {
    label: "Followers gained (% change)"
    type: number
    sql: 1.0 * (${followers_gained_total} - ${followers_gained_total_prior}) / NULLIF(${followers_gained_total_prior}, 0) ;;
    value_format_name: percent_1
  }

  measure: followers_on_day {
    label: "Followers / subscribers"
    type: sum
    sql: ${followers} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Audience stock per UTC date and Brand · Platform. Do not sum across dates or compare as a period total."
  }

  # --- Impressions -----------------------------------------------------------
  measure: total_impressions {
    label: "Total impressions"
    type: sum
    sql: ${impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Excludes YouTube (Agorapulse provides no YouTube impressions)."
  }

  measure: total_impressions_prior {
    label: "Total impressions (prior)"
    type: sum
    sql: ${impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: total_impressions_change {
    label: "Total impressions (% change)"
    type: number
    sql: 1.0 * (${total_impressions} - ${total_impressions_prior}) / NULLIF(${total_impressions_prior}, 0) ;;
    value_format_name: percent_1
  }

  measure: total_impressions_display {
    label: "Total impressions (tile)"
    type: string
    sql:
      CASE
        WHEN COUNT(CASE WHEN ${current_daily_flag} = 1 AND LOWER(${platform}) <> 'youtube' THEN 1 END) = 0
         AND COUNT(CASE WHEN ${current_daily_flag} = 1 AND LOWER(${platform}) = 'youtube' THEN 1 END) > 0
        THEN 'N/A'
        ELSE TO_CHAR(
          SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${impression_value_excl_youtube} END),
          'FM999,999,999,999,999'
        )
      END ;;
    description: "KPI tile. N/A when the result is YouTube only."
  }

  measure: total_impressions_rank {
    hidden: yes
    type: number
    sql: COALESCE(SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${impression_value_excl_youtube} END), -1) ;;
  }

  measure: organic_impressions {
    label: "Organic impressions"
    type: sum
    sql:
      CASE
        WHEN ${platform} IN ('facebook', 'instagram') THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN ${platform} IN ('tiktok', 'youtube')    THEN COALESCE(${impressions}, 0)
        ELSE 0
      END ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  measure: organic_impressions_excluding_youtube {
    label: "Organic impressions (excl. YouTube)"
    type: sum
    sql: ${organic_impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  measure: organic_impressions_excluding_youtube_prior {
    label: "Organic impressions (excl. YouTube, prior)"
    type: sum
    sql: ${organic_impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: organic_impressions_excluding_youtube_change {
    label: "Organic impressions (% change)"
    type: number
    sql: 1.0 * (${organic_impressions_excluding_youtube} - ${organic_impressions_excluding_youtube_prior}) / NULLIF(${organic_impressions_excluding_youtube_prior}, 0) ;;
    value_format_name: percent_1
  }

  measure: paid_impressions {
    label: "Paid impressions"
    type: sum
    sql: CASE WHEN ${platform} IN ('facebook', 'instagram') THEN COALESCE(${TABLE}.paid_views_count, 0) ELSE 0 END ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  # --- Video views -----------------------------------------------------------
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
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
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
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  measure: total_video_views {
    label: "Total video views"
    type: sum
    sql: ${total_video_view_value} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  measure: total_video_views_prior {
    label: "Total video views (prior)"
    type: sum
    sql: ${total_video_view_value} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: total_video_views_change {
    label: "Total video views (% change)"
    type: number
    sql: 1.0 * (${total_video_views} - ${total_video_views_prior}) / NULLIF(${total_video_views_prior}, 0) ;;
    value_format_name: percent_1
  }

  # --- Engagements -----------------------------------------------------------
  measure: total_engagements {
    label: "Total engagements"
    type: sum
    sql: ${engagements} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
  }

  measure: total_engagements_prior {
    label: "Total engagements (prior)"
    type: sum
    sql: ${engagements} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
  }

  measure: total_engagements_change {
    label: "Total engagements (% change)"
    type: number
    sql: 1.0 * (${total_engagements} - ${total_engagements_prior}) / NULLIF(${total_engagements_prior}, 0) ;;
    value_format_name: percent_1
  }

  # --- Engagement rates (change is in percentage points) ---------------------
  measure: avg_engagement_rate {
    label: "Engagement rate per view"
    type: average
    sql: ${engagement_rate_value} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: percent_2
    description: "Average of daily rates. FB/IG/TikTok use Agorapulse engagementRatePerView; YouTube uses engagements ÷ video views."
  }

  measure: avg_engagement_rate_prior {
    label: "Engagement rate per view (prior)"
    type: average
    sql: ${engagement_rate_value} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: percent_2
  }

  measure: avg_engagement_rate_change {
    label: "Engagement rate per view (pt change)"
    type: number
    sql: (${avg_engagement_rate} - ${avg_engagement_rate_prior}) * 100.0 ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
    description: "Current minus prior, in percentage points."
  }

  measure: weighted_engagement_rate {
    label: "Engagement rate (weighted)"
    type: number
    sql:
      1.0 * SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${engagements} END)
      / NULLIF(SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${impression_value_excl_youtube} END), 0) ;;
    value_format_name: percent_2
    description: "Total engagements ÷ total non-YouTube impressions."
  }

  measure: weighted_engagement_rate_prior {
    label: "Engagement rate (weighted, prior)"
    type: number
    sql:
      1.0 * SUM(CASE WHEN ${prior_daily_flag} = 1 THEN ${engagements} END)
      / NULLIF(SUM(CASE WHEN ${prior_daily_flag} = 1 THEN ${impression_value_excl_youtube} END), 0) ;;
    value_format_name: percent_2
  }

  measure: weighted_engagement_rate_change {
    label: "Engagement rate (weighted, pt change)"
    type: number
    sql: (${weighted_engagement_rate} - ${weighted_engagement_rate_prior}) * 100.0 ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
    description: "Current minus prior, in percentage points."
  }

  # ===========================================================================
  # OVERLAY METRIC SWITCHER
  # Add "Overlay metric" as a dashboard filter; one tile then switches between metrics.
  # Tile: day_of_period + overlay_value + overlay_value_prior, sorted by day_of_period.
  # ===========================================================================

  parameter: overlay_metric {
    label: "Overlay metric"
    type: unquoted
    default_value: "impressions"
    allowed_value: { label: "Organic impressions" value: "impressions" }
    allowed_value: { label: "Video views"         value: "video_views" }
    allowed_value: { label: "Engagements"         value: "engagements" }
    allowed_value: { label: "Followers gained"    value: "followers_gained" }
  }

  measure: overlay_value {
    label: "Current Period"
    label_from_parameter: overlay_metric
    type: number
    sql:
      {% if overlay_metric._parameter_value == 'video_views' %} ${total_video_views}
      {% elsif overlay_metric._parameter_value == 'engagements' %} ${total_engagements}
      {% elsif overlay_metric._parameter_value == 'followers_gained' %} ${followers_gained_total}
      {% else %} ${organic_impressions_excluding_youtube}
      {% endif %} ;;
    value_format_name: decimal_0
    description: "Current-period value of the metric chosen in Overlay metric."
  }

  measure: overlay_value_prior {
    label: "Prior Period"
    type: number
    sql:
      {% if overlay_metric._parameter_value == 'video_views' %} ${total_video_views_prior}
      {% elsif overlay_metric._parameter_value == 'engagements' %} ${total_engagements_prior}
      {% elsif overlay_metric._parameter_value == 'followers_gained' %} ${followers_gained_total_prior}
      {% else %} ${organic_impressions_excluding_youtube_prior}
      {% endif %} ;;
    value_format_name: decimal_0
    description: "Prior-period value of the metric chosen in Overlay metric."
  }

  # ===========================================================================
  # RUNNING TOTALS (cumulative charts; sort the tile by day_of_period ascending)
  # ===========================================================================

  measure: total_engagements_running {
    label: "Cumulative engagements"
    type: running_total
    sql: ${total_engagements} ;;
    value_format_name: decimal_0
  }

  measure: total_engagements_prior_running {
    label: "Cumulative engagements (prior)"
    type: running_total
    sql: ${total_engagements_prior} ;;
    value_format_name: decimal_0
  }

  measure: overlay_value_running {
    label: "Current Period (cumulative)"
    type: running_total
    sql: ${overlay_value} ;;
    value_format_name: decimal_0
  }

  measure: overlay_value_prior_running {
    label: "Prior Period (cumulative)"
    type: running_total
    sql: ${overlay_value_prior} ;;
    value_format_name: decimal_0
  }
}
