view: social_performance_v2 {
  label: "Social Performance"

  # ===========================================================================
  # SOCIAL PERFORMANCE v2
  # ===========================================================================
  # PURPOSE
  #   Single view behind the Social Performance Dashboard v2 (user-defined
  #   dashboard) and the Conversational Analytics (CA) agent. Replaces two older
  #   views: agorapulse_post_performance and social_daily_snapshot.
  #
  # SOURCES (Redshift, schema agorapulse_webhook, loaded by Segment from Agorapulse)
  #   social_post_last_30    One row per post per ingest. Agorapulse re-sends every
  #                          post from the last 30 days on each run, so a post
  #                          appears many times. Replaced social_post_snapshot,
  #                          which stopped receiving rows around 2026-07-14.
  #   social_daily_snapshot  One row per profile per UTC day per ingest. Backfills
  #                          append duplicate profile-days.
  #
  # GRAIN
  #   The derived table stacks both sources (UNION ALL) and tags each row:
  #     record_type = 'post'         latest snapshot per post_id. Metrics on these
  #                                  rows are the post's all-time (lifetime) totals.
  #     record_type = 'profile_day'  latest row per profile_id x UTC date. Metrics
  #                                  on these rows are what happened that day,
  #                                  across all posts on the profile.
  #   Every measure filters to exactly one record_type, so the two grains never
  #   add together. Post-only columns are NULL on profile-day rows and vice versa.
  #
  # DATES
  #   activity_date is the one date every filter uses:
  #     post rows         Eastern (America/New_York) publish date, matching the
  #                       Agorapulse UI.
  #     profile-day rows  UTC reporting date, as Agorapulse reports it.
  #   So "last 30 days" means posts published in those days, plus activity that
  #   happened in those days (which can include posts published earlier).
  #
  # PERIOD OVER PERIOD
  #   Two independent date filters, Current Period and Prior Period, label each
  #   row through the period dimension ('This period' / 'Prior period').
  #     Unsuffixed measures  count This period only.
  #     *_prior              count Prior period only.
  #     *_change             % change (rates: change in percentage points).
  #   The dashboard explore always sets both filters and keeps only rows in either
  #   window (sql_always_where). When Current Period is NOT set (CA explore, ad hoc
  #   use), every row is 'This period', so measures simply follow Activity date.
  #
  # CONVERSATIONAL ANALYTICS
  #   - Every visible field has a group_label and a description written for
  #     Gemini: meaning, grain, date basis, and when NOT to use it.
  #   - Labels are unique; post-lifetime measures read "Post ... (lifetime)".
  #   - Values a platform does not report are NULL, never a -1 sentinel.
  #   - The set ca_exclude lists dashboard-only fields that the CA explore removes.
  #
  # PLATFORM GAPS (Agorapulse API)
  #   YouTube   no impressions. The stored YouTube "impressions" value is video
  #             views, so every impression field returns NULL for YouTube.
  #   Instagram no separate video-view field on daily rows; views_count is used.
  #   TikTok    no organic / paid split; everything is organic, paid is 0.
  #
  # MAINTENANCE
  #   - Adding a brand alias: update brand_canonical's CASE and its suggestions.
  #   - Adding a metric: add the hidden *_value dimension, the measure with
  #     filters: [record_type, period: "This period"], and, if it needs a
  #     comparison, *_prior / *_change plus an entry in ca_exclude.
  #   - Persisted table: rebuilt when either source gets new rows
  #     (agorapulse_webhook_datagroup, defined in the explore file).
  #   - SQL uses nested subqueries, not a top-level WITH, so Looker's PDT
  #     wrapper cannot collide with it.
  # ===========================================================================
  # Persisted derived table (PDT).
  #   datagroup_trigger   rebuild when either source table receives new rows.
  #   distribution_style  ALL: the table is small, so every Redshift node keeps a
  #                       full copy and nothing is redistributed at query time.
  #   sortkeys            activity_date, because every query filters on it.
  # Column order must match exactly between the two SELECTs (UNION ALL is
  # positional). Placeholder NULLs are cast so Redshift resolves one type per
  # column; real string columns are not cast, to avoid truncating long text.
  derived_table: {
    datagroup_trigger: agorapulse_webhook_datagroup
    distribution_style: all
    sortkeys: ["activity_date"]
    sql:
      /* ---------- Branch 1: posts (one row per post_id) ---------- */
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
        0                                                                            AS is_latest_profile_day,  /* profile-day concept; 0 on posts */
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
        /* Dedupe: keep the most recently ingested snapshot of each post.
           Without this, lifetime metrics would multiply by the number of ingests. */
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

      /* ---------- Branch 2: profile-days (one row per profile per UTC date) ---------- */
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
      /* 1 on each profile's most recent reported day; drives followers_latest */
      CASE WHEN d.date::date = MAX(d.date::date) OVER (PARTITION BY d.profile_id)
      THEN 1 ELSE 0 END                                                       AS is_latest_profile_day,
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
      /* Dedupe: keep one row per profile per day. Prefer the newest payload
      schema version, then the latest ingest, because backfills re-send days. */
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

  # Dashboard-only fields removed from the CA explore (social_performance_v2_ca).
  # Rule: only list a field if no remaining field references it through ${...};
  # excluding a referenced field breaks validation. prior_period stays in for that
  # reason (helper dimensions read it through Liquid).
  set: ca_exclude {
    fields: [
      day_of_period, overlay_metric,
      platform_reports_impressions, platform_reports_video_views,
      total_posts_prior, total_posts_change,
      followers_gained_total_prior, followers_gained_total_change,
      total_impressions_prior, total_impressions_change,
      organic_impressions_prior, organic_impressions_change,
      total_video_views_prior, total_video_views_change,
      total_engagements_prior, total_engagements_change,
      avg_engagement_rate_prior, avg_engagement_rate_change,
      weighted_engagement_rate_prior, weighted_engagement_rate_change,
      overlay_value, overlay_value_prior,
      total_engagements_running, total_engagements_prior_running,
      overlay_value_running, overlay_value_prior_running
    ]
  }

  # ===========================================================================
  # KEYS & INTERNAL HELPERS (hidden)
  # ===========================================================================
  # Plumbing fields. Hidden from the field picker and from CA.

  # Primary key, unique across both grains: 'post:<post_id>' or
  # 'profile_day:<profile_id>:<date>'. Required for symmetric aggregates if this
  # view is ever joined to another.
  dimension: row_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.row_key ;;
  }

  # 'post' or 'profile_day'. Every measure filters on it. Hidden; to list posts
  # only, filter Post ID is not null.
  dimension: record_type {
    hidden: yes
    type: string
    sql: ${TABLE}.record_type ;;
  }

  # Segment event name (currently 'Social Post Last 30'). Kept for ingestion
  # debugging only.
  dimension: event {
    hidden: yes
    type: string
    sql: ${TABLE}.event ;;
  }

  # Raw brand string as stored. Use brand_canonical everywhere else.
  dimension: brand {
    hidden: yes
    type: string
    sql: ${TABLE}.brand ;;
  }

  # ===========================================================================
  # DATES
  # ===========================================================================
  # Both dimension groups use convert_tz: no. The values are already in their
  # target zone (ET for posts, converted in the PDT; UTC for profile-days), so a
  # Looker-side conversion would shift them twice.

  # The shared date (see DATES above). Every date filter and trend axis uses it.
  dimension_group: activity {
    group_label: "Date"
    label: "Activity"
    type: time
    datatype: date
    convert_tz: no
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.activity_date ;;
    description: "Default date for every social question. For post metrics (Total posts and Post … lifetime measures) it is the post's Eastern publish date. For audience, impression, video-view and engagement totals it is the UTC day the activity happened. Use this for questions like 'last month', 'in September' or 'by week'."
  }

  # Post publish timestamp in ET. NULL on profile-day rows. Use only for
  # time-of-day questions; use activity for dates.
  dimension_group: publishing {
    group_label: "Date"
    label: "Post publish"
    type: time
    datatype: timestamp
    convert_tz: no
    timeframes: [time, date, week, month, quarter, year]
    sql: ${TABLE}.publishing_ts_et ;;
    description: "Exact post publish time in Eastern time (EST/EDT). Only filled for posts; empty for audience and engagement totals. Use only when the question asks about time of day or exact publish time."
  }

  # ===========================================================================
  # PERIOD OVER PERIOD
  # ===========================================================================
  # How it works:
  #   1. current_period / prior_period are filter-only fields. They add no WHERE
  #      clause themselves; their dates are read with {% date_start %} /
  #      {% date_end %} in the hidden helper dimensions below.
  #   2. period compares activity_date to both windows and labels the row.
  #   3. Measures use filters: [period: "This period"] or ["Prior period"].
  #   4. The dashboard explore drops rows outside both windows (sql_always_where).
  # Helper dimensions are type: string because they only feed other SQL; Looker
  # would otherwise wrap a date-typed reference in timezone/format logic.

  # Filter-only. Read by period_start / period_end. Its _is_filtered flag switches
  # period between window logic and 'everything is This period'.
  filter: current_period {
    group_label: "Period comparison"
    label: "Current Period"
    type: date
    description: "Dashboard period filter. For ordinary date questions use Activity date instead. When this filter is set, measures count only rows inside it."
  }

  # Filter-only, independent of Current Period (any length, any dates). Read by
  # prior_period_start / prior_period_end.
  filter: prior_period {
    group_label: "Period comparison"
    label: "Prior Period"
    type: date
    description: "Comparison window for the dashboard's prior-period measures. Any range. If left open, it defaults to the same number of days immediately before Current Period."
  }

  # Exclusive end of Current Period. Falls back to tomorrow for open-ended
  # expressions such as 'after 2026-09-01'.
  dimension: period_end {
    hidden: yes
    type: string
    sql: COALESCE({% date_end current_period %}::date, DATEADD(day, 1, CURRENT_DATE)::date) ;;
  }

  # Start of Current Period. Falls back to 30 days before the end for
  # expressions such as 'before 2026-10-01'.
  dimension: period_start {
    hidden: yes
    type: string
    sql: COALESCE({% date_start current_period %}::date, DATEADD(day, -30, ${period_end})::date) ;;
  }

  # Current Period length in days; sizes the default Prior Period.
  dimension: days_in_period {
    hidden: yes
    type: number
    sql: DATEDIFF(day, ${period_start}, ${period_end}) ;;
  }

  # Exclusive end of Prior Period. Default: the day Current Period starts.
  dimension: prior_period_end {
    hidden: yes
    type: string
    sql: COALESCE({% date_end prior_period %}::date, ${period_start}) ;;
  }

  # Start of Prior Period. Default: same length as Current Period, ending where
  # it begins.
  dimension: prior_period_start {
    hidden: yes
    type: string
    sql: COALESCE({% date_start prior_period %}::date, DATEADD(day, -(${days_in_period})::int, ${prior_period_end})::date) ;;
  }

  # Labels each row 'This period', 'Prior period' or NULL (outside both).
  # If the windows overlap, overlapping days count as 'This period' (first match).
  # Without a Current Period filter every row is 'This period', so measures follow
  # ordinary Activity date filters (CA, ad hoc Explores).
  dimension: period {
    group_label: "Period comparison"
    label: "Period"
    type: string
    suggestions: ["This period", "Prior period"]
    sql:
      {% if current_period._is_filtered %}
      CASE
        WHEN ${activity_raw} >= ${period_start}       AND ${activity_raw} < ${period_end}       THEN 'This period'
        WHEN ${activity_raw} >= ${prior_period_start} AND ${activity_raw} < ${prior_period_end} THEN 'Prior period'
      END
      {% else %}
      'This period'
      {% endif %} ;;
    description: "Dashboard helper. 'This period' is inside Current Period and 'Prior period' is inside Prior Period. When Current Period is not set, every row is 'This period'. Do not use for ordinary date questions."
  }

  # 1..N within each window, so the current and prior periods line up on one
  # x-axis for overlay charts even though their calendar dates differ.
  dimension: day_of_period {
    group_label: "Period comparison"
    label: "Day of period"
    type: number
    sql:
      DATEDIFF(
        day,
        CASE WHEN ${period} = 'This period' THEN ${period_start} ELSE ${prior_period_start} END,
        ${activity_raw}
      ) + 1 ;;
    description: "Day 1 to N inside each period window. X-axis for charts that overlay the current period on the prior one."
  }

  # 1 for profile-day rows in This period. Used inside type: number measures,
  # which cannot use filters:.
  dimension: current_daily_flag {
    hidden: yes
    type: number
    sql: CASE WHEN ${record_type} = 'profile_day' AND ${period} = 'This period' THEN 1 ELSE 0 END ;;
  }

  # 1 for profile-day rows in Prior period. Same purpose as current_daily_flag.
  dimension: prior_daily_flag {
    hidden: yes
    type: number
    sql: CASE WHEN ${record_type} = 'profile_day' AND ${period} = 'Prior period' THEN 1 ELSE 0 END ;;
  }

  # ===========================================================================
  # BRAND & PLATFORM
  # ===========================================================================
  # Shared by both grains, so Brand and Platform filters apply to every tile.
  # Suggestions are hardcoded: a suggestion query would SELECT DISTINCT over the
  # PDT and is slow.

  # Normalizes brand spellings so UPFF and UP Faith & Family are one brand.
  # Heartland on UP Faith & Family stays separate. To add an alias, update the
  # CASE and the suggestions list together.
  dimension: brand_canonical {
    group_label: "Brand & platform"
    label: "Brand"
    type: string
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
    description: "Network brand that owns the social profile. Values: Aspire TV, Heartland on UP Faith & Family, Ovation TV, UP Faith & Family (also called UPFF), Uplift Someone, UPtv. 'Heartland on UP Faith & Family' is its own brand and is not included in UP Faith & Family."
  }

  # Lowercase platform name from Agorapulse.
  dimension: platform {
    group_label: "Brand & platform"
    label: "Platform"
    type: string
    suggestions: ["facebook", "instagram", "tiktok", "youtube"]
    sql: ${TABLE}.platform ;;
    description: "Social network, stored lowercase: facebook, instagram, tiktok, youtube. Also called channel or network."
  }

  # Series key for the followers trend chart, e.g. 'UPtv · Facebook'.
  dimension: brand_platform {
    group_label: "Brand & platform"
    label: "Brand · Platform"
    type: string
    sql: ${brand_canonical} || ' · ' || INITCAP(${platform}) ;;
    description: "Brand and platform combined, for example 'UPtv · Facebook'. Use to break follower counts out by account."
  }

  # Agorapulse account name. Profile-day rows only.
  dimension: profile_name {
    group_label: "Brand & platform"
    label: "Profile name"
    type: string
    sql: ${TABLE}.profile_name ;;
    description: "Agorapulse profile (account) name. Only filled for audience and engagement totals, not for posts."
  }

  # Agorapulse account ID. Profile-day rows only.
  dimension: profile_id {
    group_label: "Brand & platform"
    label: "Profile ID"
    type: string
    sql: ${TABLE}.profile_id ;;
    description: "Agorapulse profile ID. Only filled for audience and engagement totals."
  }

  # Chart helper. Separate from platform so the dashboard Platform filter cannot
  # override it. Visible because user-defined dashboard tile filters cannot use
  # hidden fields. Excluded from CA.
  dimension: platform_reports_impressions {
    group_label: "Chart filters"
    label: "Platform reports impressions"
    type: yesno
    sql: LOWER(${TABLE}.platform) <> 'youtube' ;;
    description: "Yes for platforms with an impression metric (all except YouTube). Tile filter for impression charts."
  }

  # Chart helper for the video views chart (Instagram has no daily video-view
  # field). Same reasoning as platform_reports_impressions.
  dimension: platform_reports_video_views {
    group_label: "Chart filters"
    label: "Platform shown on video views chart"
    type: yesno
    sql: LOWER(${TABLE}.platform) <> 'instagram' ;;
    description: "Yes for platforms shown on the video views chart (all except Instagram). Tile filter."
  }

  # ===========================================================================
  # POST DIMENSIONS
  # ===========================================================================
  # Filled on post rows only (NULL on profile-day rows).

  # Agorapulse post ID. NULL on profile-day rows, which makes 'is not null' a
  # simple posts-only filter.
  dimension: post_id {
    group_label: "Post"
    label: "Post ID"
    type: string
    sql: ${TABLE}.post_id ;;
    description: "Agorapulse post ID. Only filled for posts. Filter 'Post ID is not null' to list posts only."
  }

  # Caption/title; falls back to post_id for image-only posts with no text.
  dimension: post_text {
    group_label: "Post"
    label: "Post text"
    type: string
    sql: COALESCE(NULLIF(TRIM(${TABLE}.post_text), ''), ${TABLE}.post_id) ;;
    description: "Post caption or title. Falls back to the post ID when the post has no text. Use when the question asks which post, or names a post by its words."
  }

  # Permalink; html renders it as a link that opens in a new tab.
  dimension: post_url {
    group_label: "Post"
    label: "Post URL"
    type: string
    sql: ${TABLE}.post_url ;;
    html:
      {% if value != blank %}
        <a href="{{ value }}" target="_blank" rel="noopener noreferrer">{{ value }}</a>
      {% else %}
        {{ rendered_value }}
      {% endif %} ;;
    description: "Link to the post on its platform."
  }

  # Per-post total impressions, mapped per platform from the Agorapulse content
  # report: Instagram impressions_count, Facebook/TikTok views_count (those
  # platforms leave impressions_count empty), YouTube NULL.
  dimension: post_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN NULL
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.impressions_count, 0)
        WHEN LOWER(${platform}) IN ('facebook', 'tiktok') THEN COALESCE(${TABLE}.views_count, 0)
        ELSE COALESCE(${TABLE}.impressions_count, ${TABLE}.views_count, 0)
      END ;;
  }

  # Organic share of post_impression_value: Instagram organic_impressions_count,
  # Facebook organic_views_count, TikTok views_count (no split), YouTube NULL.
  dimension: post_organic_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN NULL
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.organic_impressions_count, 0)
        WHEN LOWER(${platform}) = 'facebook' THEN COALESCE(${TABLE}.organic_views_count, 0)
        WHEN LOWER(${platform}) = 'tiktok' THEN COALESCE(${TABLE}.views_count, 0)
        ELSE COALESCE(${TABLE}.organic_impressions_count, ${TABLE}.organic_views_count, 0)
      END ;;
  }

  # Paid share of post_impression_value: Instagram paid_impressions_count,
  # Facebook paid_views_count, TikTok 0, YouTube NULL. Organic + paid = total.
  dimension: post_paid_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN NULL
        WHEN LOWER(${platform}) = 'instagram' THEN COALESCE(${TABLE}.paid_impressions_count, 0)
        WHEN LOWER(${platform}) = 'facebook' THEN COALESCE(${TABLE}.paid_views_count, 0)
        WHEN LOWER(${platform}) = 'tiktok' THEN 0
        ELSE COALESCE(${TABLE}.paid_impressions_count, ${TABLE}.paid_views_count, 0)
      END ;;
  }

  # ===========================================================================
  # PROFILE-DAY VALUE DIMENSIONS (hidden)
  # ===========================================================================
  # Raw columns and per-platform value mappings for the profile-day measures.
  # Hidden: users and CA reach them only through the measures.

  # Raw columns. All profile-day only, except engagements, which on post rows holds
  # the post's lifetime engagement_count (read by post_engagements).
  dimension: impressions          { hidden: yes type: number sql: ${TABLE}.impressions ;; }
  dimension: video_views          { hidden: yes type: number sql: ${TABLE}.video_views ;; }
  dimension: engagements          { hidden: yes type: number sql: ${TABLE}.engagements ;; }
  dimension: followers            { hidden: yes type: number sql: ${TABLE}.followers ;; }
  dimension: followers_gained     { hidden: yes type: number sql: ${TABLE}.followers_gained ;; }
  dimension: engagement_rate_per_view { hidden: yes type: number sql: ${TABLE}.engagement_rate_per_view ;; }

  # 1 on each profile's most recent reported day (computed in the PDT).
  dimension: is_latest_profile_day {
    hidden: yes
    type: number
    sql: ${TABLE}.is_latest_profile_day ;;
  }

  # Profile-day impressions with YouTube removed (YouTube's stored value is video
  # views, not impressions).
  dimension: impression_value_excl_youtube {
    hidden: yes
    type: number
    sql: CASE WHEN LOWER(${platform}) = 'youtube' THEN NULL ELSE ${impressions} END ;;
  }

  # Profile-day organic impressions: Facebook/Instagram organic_views_count,
  # TikTok impressions (all organic), YouTube NULL.
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

  # Profile-day paid impressions: Facebook/Instagram paid_views_count, TikTok 0,
  # YouTube NULL.
  dimension: paid_impression_value {
    hidden: yes
    type: number
    sql:
      CASE
        WHEN LOWER(${platform}) = 'youtube' THEN NULL
        WHEN ${platform} IN ('facebook', 'instagram') THEN COALESCE(${TABLE}.paid_views_count, 0)
        ELSE 0
      END ;;
  }

  # Profile-day video views: Facebook/YouTube video_views_count,
  # Instagram/TikTok views_count.
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

  # Daily engagement rate. Agorapulse stores engagementRatePerView as a percent
  # (2.56 = 2.56%), so divide by 100. YouTube has no rate; fall back to
  # engagements / video views.
  dimension: engagement_rate_value {
    hidden: yes
    type: number
    sql: COALESCE(${engagement_rate_per_view} / 100.0, 1.0 * ${engagements} / NULLIF(${video_views}, 0)) ;;
  }

  # ===========================================================================
  # POST MEASURES
  # ===========================================================================
  # Post-grain measures. Each post has exactly one row, so type: max returns that
  # post's value; max (not sum) also keeps a post from double counting if the
  # query grain ever widens. Use with Post text / Post ID, not for period totals.

  # count_distinct on post_id (NULL on profile-day rows, so those are ignored).
  measure: total_posts {
    group_label: "Post"
    label: "Total posts"
    type: count_distinct
    sql: ${post_id} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "Number of posts published, counted by Eastern publish date. Also called post count or posts published."
  }

  # Sort key for Top 20. Tile needs 'Post impressions is not null', because
  # Redshift sorts NULLs (YouTube) first in descending order.
  measure: post_impressions {
    group_label: "Post"
    label: "Post impressions (lifetime)"
    type: max
    sql: ${post_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "All-time impressions of each post (organic plus paid), from its latest Agorapulse snapshot. Use with Post text or Post ID to rank top posts. Do not add up across many posts for a period total; use Total impressions for that. Empty for YouTube, which has no impressions."
  }

  # Shown in Top 20 alongside paid; organic + paid = post_impressions.
  measure: post_organic_impressions {
    group_label: "Post"
    label: "Post organic impressions (lifetime)"
    type: max
    sql: ${post_organic_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "All-time organic (unpaid) impressions of each post. Use per post only. Empty for YouTube."
  }

  # Paid (boosted) share per post.
  measure: post_paid_impressions {
    group_label: "Post"
    label: "Post paid impressions (lifetime)"
    type: max
    sql: ${post_paid_impression_value} ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "All-time paid (boosted) impressions of each post. Use per post only. TikTok is always 0; empty for YouTube."
  }

  # Lifetime engagements per post.
  measure: post_engagements {
    group_label: "Post"
    label: "Post engagements (lifetime)"
    type: max
    sql: COALESCE(${engagements}, 0) ;;
    filters: [record_type: "post", period: "This period"]
    value_format_name: decimal_0
    description: "All-time engagements (reactions, comments, shares, clicks) of each post. Use per post only; use Total engagements for period totals."
  }

  # Lifetime video views per post. NULLIF(...,0) shows photo posts as empty
  # instead of 0. Instagram falls back to impressions_count when views_count is
  # missing, matching the daily mapping.
  measure: post_video_views {
    group_label: "Post"
    label: "Post video views (lifetime)"
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
    description: "All-time video views of each post. Empty for photo posts. Use per post only; use Total video views for period totals."
  }

  # ===========================================================================
  # AUDIENCE MEASURES
  # ===========================================================================
  # Profile-day grain. followers is a stock (count on a day); followers_gained is
  # a flow (gained that day). Only flows can be summed across days.

  # Flow: safe to sum across days. Gross (no unfollows).
  measure: followers_gained_total {
    group_label: "Audience"
    label: "Followers gained"
    type: sum
    sql: ${followers_gained} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "New followers (YouTube: subscribers) gained during the dates, before unfollows. Gross growth, not net. Also called new followers."
  }

  # Each profile's count on its latest reported day. Not tied to Current Period.
  # A date filter that excludes the latest day returns empty, so use it without a
  # date filter or with one that includes yesterday.
  measure: followers_latest {
    group_label: "Audience"
    label: "Followers / subscribers (current)"
    type: sum
    sql: ${followers} ;;
    filters: [record_type: "profile_day", is_latest_profile_day: "1"]
    value_format_name: decimal_0
    description: "Current audience size: each profile's follower (YouTube: subscriber) count on its latest reported day. Use for 'how many followers does X have'. Use without a date filter, or with dates that include the most recent day; otherwise it returns empty. For history use Followers / subscribers (daily) by Activity date."
  }

  # Stock: correct per day only. Group by Activity date; never total across days.
  measure: followers_on_day {
    group_label: "Audience"
    label: "Followers / subscribers (daily)"
    type: sum
    sql: ${followers} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Audience size on each day. Always group by Activity date; summing across several days gives a meaningless number. For a single current number use Followers / subscribers (current)."
  }

  # ===========================================================================
  # IMPRESSIONS & VIDEO VIEWS
  # ===========================================================================
  # Profile-day grain: activity on the day it happened, across all posts.
  # Organic + paid = total for each platform where Agorapulse splits them.

  # All non-YouTube impressions on the day they happened.
  measure: total_impressions {
    group_label: "Impressions & views"
    label: "Total impressions"
    type: sum
    sql: ${impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Impressions (organic plus paid) on the day they happened, across all posts. Facebook, Instagram and TikTok only; YouTube has no impressions in Agorapulse and is empty. Use for impression totals and trends."
  }

  # Used by the brand, channel, trend and overlay tiles. Replaces the old
  # organic_impressions_excluding_youtube; the old organic_impressions (which
  # counted YouTube video views as impressions) was removed.
  measure: organic_impressions {
    group_label: "Impressions & views"
    label: "Organic impressions"
    type: sum
    sql: ${organic_impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Unpaid impressions on the day they happened. Facebook, Instagram and TikTok only; YouTube is empty. Organic plus Paid impressions equals Total impressions."
  }

  # Paid share of total_impressions.
  measure: paid_impressions {
    group_label: "Impressions & views"
    label: "Paid impressions"
    type: sum
    sql: ${paid_impression_value} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Paid (boosted or ad) impressions on the day they happened. Facebook and Instagram only; TikTok is 0 and YouTube is empty."
  }

  # Main reach metric for YouTube.
  measure: total_video_views {
    group_label: "Impressions & views"
    label: "Total video views"
    type: sum
    sql: ${total_video_view_value} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Video views on the day they happened, all platforms. Use for YouTube reach, since YouTube has no impressions. Organic plus Paid video views equals this."
  }

  # Facebook organic_video_views_count, Instagram organic_views_count,
  # TikTok/YouTube all views (no split).
  measure: organic_video_views {
    group_label: "Impressions & views"
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
    description: "Unpaid video views on the day they happened."
  }

  # Facebook paid_video_views_count, Instagram paid_views_count, else 0.
  measure: paid_video_views {
    group_label: "Impressions & views"
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
    description: "Paid video views on the day they happened. Facebook and Instagram only; TikTok and YouTube are 0."
  }

  # ===========================================================================
  # ENGAGEMENT
  # ===========================================================================
  # Two engagement rates, deliberately different:
  #   avg_engagement_rate       average of daily rates (matches Agorapulse UI and
  #                             the dashboard tile). Default.
  #   weighted_engagement_rate  total engagements / total impressions.

  # Profile-day engagements; use for totals and trends (not post_engagements).
  measure: total_engagements {
    group_label: "Engagement"
    label: "Total engagements"
    type: sum
    sql: ${engagements} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: decimal_0
    description: "Engagements (reactions, comments, shares, clicks) on the day they happened, across all posts. Use for engagement totals and trends."
  }

  # type: average over profile-days in the window.
  measure: avg_engagement_rate {
    group_label: "Engagement"
    label: "Engagement rate per view"
    type: average
    sql: ${engagement_rate_value} ;;
    filters: [record_type: "profile_day", period: "This period"]
    value_format_name: percent_2
    description: "Default engagement rate, matching the dashboard. Average of each profile-day's engagements per view, so quiet days count the same as busy days. Facebook, Instagram and TikTok use Agorapulse's rate; YouTube uses engagements divided by video views."
  }

  # type: number with manual CASE filters (filters: is not allowed on
  # type: number). NULL denominator for YouTube-only results.
  measure: weighted_engagement_rate {
    group_label: "Engagement"
    label: "Engagement rate (weighted)"
    type: number
    sql:
      1.0 * SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${engagements} END)
      / NULLIF(SUM(CASE WHEN ${current_daily_flag} = 1 THEN ${impression_value_excl_youtube} END), 0) ;;
    value_format_name: percent_2
    description: "Total engagements divided by total impressions. Use only when asked for a weighted or overall rate. Excludes YouTube, so a YouTube-only result is empty."
  }

  # ===========================================================================
  # PERIOD COMPARISON MEASURES (dashboard only; removed from the CA explore)
  # ===========================================================================
  # *_prior  same SQL as the current measure, filtered to 'Prior period'.
  # *_change (current - prior) / prior. 1.0 * forces decimal division (Redshift
  #          divides integers as integers); NULLIF avoids divide-by-zero, so a
  #          prior of 0 returns NULL instead of an error.
  # Rate changes are current - prior, x100, shown as percentage points.
  # Every field here is listed in ca_exclude.

  # Prior-period twin of total_posts.
  measure: total_posts_prior {
    group_label: "Period comparison"
    label: "Total posts (prior)"
    type: count_distinct
    sql: ${post_id} ;;
    filters: [record_type: "post", period: "Prior period"]
    value_format_name: decimal_0
    description: "Total posts in the Prior Period."
  }

  # % change in total_posts, current vs prior.
  measure: total_posts_change {
    group_label: "Period comparison"
    label: "Total posts (% change)"
    type: number
    sql: 1.0 * (${total_posts} - ${total_posts_prior}) / NULLIF(${total_posts_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Total posts, Current vs Prior Period."
  }

  # Prior-period twin of followers_gained_total.
  measure: followers_gained_total_prior {
    group_label: "Period comparison"
    label: "Followers gained (prior)"
    type: sum
    sql: ${followers_gained} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
    description: "Followers gained in the Prior Period."
  }

  # % change in followers_gained_total, current vs prior.
  measure: followers_gained_total_change {
    group_label: "Period comparison"
    label: "Followers gained (% change)"
    type: number
    sql: 1.0 * (${followers_gained_total} - ${followers_gained_total_prior}) / NULLIF(${followers_gained_total_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Followers gained, Current vs Prior Period."
  }

  # Prior-period twin of total_impressions.
  measure: total_impressions_prior {
    group_label: "Period comparison"
    label: "Total impressions (prior)"
    type: sum
    sql: ${impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
    description: "Total impressions in the Prior Period."
  }

  # % change in total_impressions, current vs prior.
  measure: total_impressions_change {
    group_label: "Period comparison"
    label: "Total impressions (% change)"
    type: number
    sql: 1.0 * (${total_impressions} - ${total_impressions_prior}) / NULLIF(${total_impressions_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Total impressions, Current vs Prior Period."
  }

  # Prior-period twin of organic_impressions.
  measure: organic_impressions_prior {
    group_label: "Period comparison"
    label: "Organic impressions (prior)"
    type: sum
    sql: ${organic_impression_value_excl_youtube} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
    description: "Organic impressions in the Prior Period."
  }

  # % change in organic_impressions, current vs prior.
  measure: organic_impressions_change {
    group_label: "Period comparison"
    label: "Organic impressions (% change)"
    type: number
    sql: 1.0 * (${organic_impressions} - ${organic_impressions_prior}) / NULLIF(${organic_impressions_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Organic impressions, Current vs Prior Period."
  }

  # Prior-period twin of total_video_views.
  measure: total_video_views_prior {
    group_label: "Period comparison"
    label: "Total video views (prior)"
    type: sum
    sql: ${total_video_view_value} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
    description: "Total video views in the Prior Period."
  }

  # % change in total_video_views, current vs prior.
  measure: total_video_views_change {
    group_label: "Period comparison"
    label: "Total video views (% change)"
    type: number
    sql: 1.0 * (${total_video_views} - ${total_video_views_prior}) / NULLIF(${total_video_views_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Total video views, Current vs Prior Period."
  }

  # Prior-period twin of total_engagements.
  measure: total_engagements_prior {
    group_label: "Period comparison"
    label: "Total engagements (prior)"
    type: sum
    sql: ${engagements} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: decimal_0
    description: "Total engagements in the Prior Period."
  }

  # % change in total_engagements, current vs prior.
  measure: total_engagements_change {
    group_label: "Period comparison"
    label: "Total engagements (% change)"
    type: number
    sql: 1.0 * (${total_engagements} - ${total_engagements_prior}) / NULLIF(${total_engagements_prior}, 0) ;;
    value_format_name: percent_1
    description: "Percent change in Total engagements, Current vs Prior Period."
  }

  # Prior-period twin of avg_engagement_rate.
  measure: avg_engagement_rate_prior {
    group_label: "Period comparison"
    label: "Engagement rate per view (prior)"
    type: average
    sql: ${engagement_rate_value} ;;
    filters: [record_type: "profile_day", period: "Prior period"]
    value_format_name: percent_2
    description: "Engagement rate per view in the Prior Period."
  }

  # Current vs prior avg_engagement_rate, in percentage points.
  measure: avg_engagement_rate_change {
    group_label: "Period comparison"
    label: "Engagement rate per view (pt change)"
    type: number
    sql: (${avg_engagement_rate} - ${avg_engagement_rate_prior}) * 100.0 ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
    description: "Current minus prior engagement rate per view, in percentage points."
  }

  # Prior-period twin of weighted_engagement_rate.
  measure: weighted_engagement_rate_prior {
    group_label: "Period comparison"
    label: "Engagement rate (weighted, prior)"
    type: number
    sql:
      1.0 * SUM(CASE WHEN ${prior_daily_flag} = 1 THEN ${engagements} END)
      / NULLIF(SUM(CASE WHEN ${prior_daily_flag} = 1 THEN ${impression_value_excl_youtube} END), 0) ;;
    value_format_name: percent_2
    description: "Weighted engagement rate in the Prior Period."
  }

  # Current vs prior weighted_engagement_rate, in percentage points.
  measure: weighted_engagement_rate_change {
    group_label: "Period comparison"
    label: "Engagement rate (weighted, pt change)"
    type: number
    sql: (${weighted_engagement_rate} - ${weighted_engagement_rate_prior}) * 100.0 ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
    description: "Current minus prior weighted engagement rate, in percentage points."
  }

  # ===========================================================================
  # OVERLAY METRIC SWITCHER + RUNNING TOTALS (dashboard only)
  # ===========================================================================
  # overlay_metric is a parameter: picking a value in the dashboard filter
  # rewrites the overlay measures' SQL through Liquid, so one tile can show any
  # of four metrics without custom fields.
  # running_total measures are computed by Looker after the query returns, in
  # row order: sort the tile by day_of_period ascending. They cannot be used as
  # filters and are not available to CA.

  # Values map to the measures in overlay_value / overlay_value_prior.
  parameter: overlay_metric {
    group_label: "Period comparison"
    label: "Overlay metric"
    type: unquoted
    default_value: "impressions"
    allowed_value: { label: "Organic impressions" value: "impressions" }
    allowed_value: { label: "Video views"         value: "video_views" }
    allowed_value: { label: "Engagements"         value: "engagements" }
    allowed_value: { label: "Followers gained"    value: "followers_gained" }
    description: "Chooses the metric shown by the overlay and cumulative charts."
  }

  # label_from_parameter makes the series name match the chosen metric.
  measure: overlay_value {
    group_label: "Period comparison"
    label: "Current Period"
    label_from_parameter: overlay_metric
    type: number
    sql:
      {% if overlay_metric._parameter_value == 'video_views' %} ${total_video_views}
      {% elsif overlay_metric._parameter_value == 'engagements' %} ${total_engagements}
      {% elsif overlay_metric._parameter_value == 'followers_gained' %} ${followers_gained_total}
      {% else %} ${organic_impressions}
      {% endif %} ;;
    value_format_name: decimal_0
    description: "Current-period value of the metric chosen in Overlay metric."
  }

  # Prior-period twin of overlay_value.
  measure: overlay_value_prior {
    group_label: "Period comparison"
    label: "Prior Period"
    type: number
    sql:
      {% if overlay_metric._parameter_value == 'video_views' %} ${total_video_views_prior}
      {% elsif overlay_metric._parameter_value == 'engagements' %} ${total_engagements_prior}
      {% elsif overlay_metric._parameter_value == 'followers_gained' %} ${followers_gained_total_prior}
      {% else %} ${organic_impressions_prior}
      {% endif %} ;;
    value_format_name: decimal_0
    description: "Prior-period value of the metric chosen in Overlay metric."
  }

  # Cumulative engagements, current period.
  measure: total_engagements_running {
    group_label: "Period comparison"
    label: "Cumulative engagements"
    type: running_total
    sql: ${total_engagements} ;;
    value_format_name: decimal_0
    description: "Running total of Total engagements in row order. Sort by Day of period."
  }

  # Cumulative engagements, prior period.
  measure: total_engagements_prior_running {
    group_label: "Period comparison"
    label: "Cumulative engagements (prior)"
    type: running_total
    sql: ${total_engagements_prior} ;;
    value_format_name: decimal_0
    description: "Running total of prior-period engagements in row order. Sort by Day of period."
  }

  # Cumulative overlay metric, current period.
  measure: overlay_value_running {
    group_label: "Period comparison"
    label: "Current Period (cumulative)"
    type: running_total
    sql: ${overlay_value} ;;
    value_format_name: decimal_0
    description: "Running total of the Overlay metric, current period. Sort by Day of period."
  }

  # Cumulative overlay metric, prior period.
  measure: overlay_value_prior_running {
    group_label: "Period comparison"
    label: "Prior Period (cumulative)"
    type: running_total
    sql: ${overlay_value_prior} ;;
    value_format_name: decimal_0
    description: "Running total of the Overlay metric, prior period. Sort by Day of period."
  }
}
