# =============================================================================
# UP Faith & Family Sign-Up Funnel (iOS + Android + Connected TV + Web): Current vs Prior
# Optimized for Looker Conversational Analytics:
#   - Every visible field has a plain-language label and a description that
#     defines it, says when to use it, and lists common synonyms.
#   - Technical helper fields are hidden so the agent only sees business fields.
#   - Filterable dimensions list their valid values (suggestions).
#   - Rates are stored as ratios and formatted as percentages; changes are in
#     percentage points and say so in the label.
#   - Web entries carry the UTM campaign fields (source, name, medium, content,
#     term) and a Marketing Platform bucket from the user's FIRST page view in
#     each period. The "Web Campaign Filters" apply to Web users only: mobile
#     (iOS, Android) and Connected TV (Roku, Amazon Fire TV, Tizen TV) users
#     always pass through, so app data stays in every result.
#     The filters run inside the derived table, so every metric (including the
#     average daily rates) reflects them. App rows show "Mobile App (no UTM)" or
#     "Connected TV (no UTM)".
#
#   Step | iOS / Android / CTV      | Web
#   -----+--------------------------+-------------------------------------------
#   CTV  = Roku (roku), Amazon Fire TV (amazon_fire_tv), Tizen TV (tizen_tv);
#          same Segment app tables as ios and android.
#   1    | App Installed            | Landing Page Visit (/stream/, /subscribe/)
#   2    | Sign Up Viewed           | Product Viewed
#   3    | Subscription Plan Chosen | Signed Up
#   4    | Order Completed          | Order Completed
#
# Grain: one row per user per funnel step. A "user" is an anonymous_id per
# platform per period (first entry on that platform in the period's date range);
# steps are 1-4 plus an OVERALL row. User counts are distinct, so step rows
# never inflate totals. Dialect: Redshift.
# =============================================================================

view: upff_signup_funnel {
  label: "UPFF Sign-Up Funnel"

  # ---------------------------------------------------------------------------
  # Inputs: comparison periods and attribution window
  # ---------------------------------------------------------------------------

  filter: current_period {
    type: date
    label: "Current Period"
    description: "Date range of entries (app installs or landing page visits) for the current period being analyzed. Defaults to the last 7 full days. Examples: 'this week', 'this month', 'last 30 days', '2026/09/20 to 2026/09/27'. Also called: this period, current week, current month, selected period."
  }

  filter: prior_period {
    type: date
    label: "Prior Period"
    description: "Date range of entries for the comparison period. Defaults to the 7 days before the current period. Examples: 'last week', 'last month', or the same dates last year for a year-over-year comparison. Also called: previous period, comparison period, last week, baseline."
  }

  parameter: attribution_days {
    type: unquoted
    label: "Attribution Window (Days)"
    description: "Number of days after entry that a user has to complete each funnel step and still count as converted. Default is 3 days. Also called: conversion window, lookback window."
    allowed_value: { label: "1 day"   value: "1" }
    allowed_value: { label: "3 days"  value: "3" }
    allowed_value: { label: "7 days"  value: "7" }
    allowed_value: { label: "14 days" value: "14" }
    allowed_value: { label: "30 days" value: "30" }
    default_value: "3"
  }

  # ---------------------------------------------------------------------------
  # Web campaign filters (apply to Web users only; app users always included)
  # ---------------------------------------------------------------------------

  filter: marketing_platform_filter {
    type: string
    group_label: "Web Campaign Filters (Web only)"
    label: "Marketing Platform Filter"
    description: "Limits WEB users to those who arrived from the selected marketing platform(s), e.g. Meta Ads or Google Search. iOS, Android and Connected TV users are not affected and stay in every result. Use this (not the Marketing Platform dimension) to filter by channel."
    suggestions: ["Google Search", "Google PMax", "Google Display", "YouTube", "Meta Ads", "Bing Ads", "HubSpot", "UPtv Digital", "ChatGPT", "Organic Search", "Organic Social", "Others", "Unknown"]
  }

  filter: campaign_source_filter {
    type: string
    group_label: "Web Campaign Filters (Web only)"
    label: "Campaign Source Filter"
    description: "Limits WEB users to those whose arriving visit had the selected utm_source(s). iOS, Android and Connected TV users are not affected. Use this (not the Campaign Source dimension) to filter by source."
    suggest_dimension: campaign_source
  }

  filter: campaign_name_filter {
    type: string
    group_label: "Web Campaign Filters (Web only)"
    label: "Campaign Name Filter"
    description: "Limits WEB users to those whose arriving visit had the selected utm_campaign(s). iOS, Android and Connected TV users are not affected. Use this (not the Campaign Name dimension) to filter by campaign."
    suggest_dimension: campaign_name
  }

  filter: campaign_medium_filter {
    type: string
    group_label: "Web Campaign Filters (Web only)"
    label: "Campaign Medium Filter"
    description: "Limits WEB users to those whose arriving visit had the selected utm_medium(s), e.g. cpc or email. iOS, Android and Connected TV users are not affected."
    suggest_dimension: campaign_medium
  }

  derived_table: {
    sql:
      WITH

            -- ---------- Unioned event sources ----------
            -- Web carries Segment UTM fields (context_campaign_*); mobile and CTV apps have none.
            entry_events AS (
                SELECT 'iOS'::VARCHAR(32) AS platform, anonymous_id, received_at,
                       CAST(NULL AS VARCHAR(512)) AS campaign_source,
                       CAST(NULL AS VARCHAR(512)) AS campaign_name,
                       CAST(NULL AS VARCHAR(512)) AS campaign_medium,
                       CAST(NULL AS VARCHAR(512)) AS campaign_content,
                       CAST(NULL AS VARCHAR(512)) AS campaign_term,
                       'Mobile App (no UTM)'::VARCHAR(64) AS marketing_platform
                FROM ios.app_installed
                UNION ALL
                SELECT 'Android', anonymous_id, received_at,
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       'Mobile App (no UTM)'::VARCHAR(64)
                FROM android.app_installed
                UNION ALL
                SELECT 'Roku', anonymous_id, received_at,
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       'Connected TV (no UTM)'::VARCHAR(64)
                FROM roku.app_installed
                UNION ALL
                SELECT 'Amazon Fire TV', anonymous_id, received_at,
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       'Connected TV (no UTM)'::VARCHAR(64)
                FROM amazon_fire_tv.app_installed
                UNION ALL
                SELECT 'Tizen TV', anonymous_id, received_at,
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       CAST(NULL AS VARCHAR(512)), CAST(NULL AS VARCHAR(512)),
                       'Connected TV (no UTM)'::VARCHAR(64)
                FROM tizen_tv.app_installed
                UNION ALL
                SELECT 'Web', anonymous_id, received_at,
                       context_campaign_source::VARCHAR(512),
                       context_campaign_name::VARCHAR(512),
                       context_campaign_medium::VARCHAR(512),
                       context_campaign_content::VARCHAR(512),
                       context_campaign_term::VARCHAR(512),
                       CASE
                         WHEN LOWER(context_campaign_source) IN ('google','google_ads','adwords')
                              AND LOWER(context_campaign_medium) IN ('cpc','ppc','paid','g')
                              AND LOWER(context_campaign_name) LIKE '%display%'                       THEN 'Google Display'
                         WHEN LOWER(context_campaign_source) = 'youtube'                              THEN 'YouTube'
                         WHEN LOWER(context_campaign_source) = 'chatgpt.com'                          THEN 'ChatGPT'
                         WHEN LOWER(context_campaign_source) IN ('google','google_ads','adwords')
                              AND LOWER(context_campaign_medium) IN ('cpc','ppc','paid','g')
                              AND (LOWER(context_campaign_name) LIKE '%pmax%'
                                   OR LOWER(context_campaign_name) LIKE '%performance max%')          THEN 'Google PMax'
                         WHEN LOWER(context_campaign_source) IN ('google','google_ads','adwords')
                              AND LOWER(context_campaign_medium) IN ('cpc','ppc','paid','g')          THEN 'Google Search'
                         WHEN LOWER(context_campaign_source) IN ('facebook','meta','ig','fb','an','fb-sitelink','th','msg',
                                                                 'site_source_name','site.source.name','campaign.name')
                              OR LOWER(context_campaign_source) LIKE 'meta%'                          THEN 'Meta Ads'
                         WHEN LOWER(context_campaign_source) IN ('bing','bing_ads','microsoft','msn') THEN 'Bing Ads'
                         WHEN LOWER(context_campaign_source) IN ('hubspot','hubspot_upff','hubspot_uptv')
                              OR LOWER(context_campaign_medium) LIKE 'email%'                         THEN 'HubSpot'
                         WHEN LOWER(context_campaign_source) IN ('uptv','uptv_movies_app')            THEN 'UPtv Digital'
                         WHEN LOWER(context_campaign_medium) = 'organic'
                              AND LOWER(context_campaign_source) IN ('google','bing','duckduckgo','yahoo') THEN 'Organic Search'
                         WHEN LOWER(context_campaign_medium) IN ('social','organic_social')
                              OR (LOWER(context_campaign_medium) = 'organic'
                                  AND LOWER(context_campaign_source) IN ('facebook','instagram','tiktok','x','twitter','linkedin'))
                                                                                                      THEN 'Organic Social'
                         WHEN LOWER(context_campaign_source) IN ('organic','direct')                  THEN 'Others'
                         WHEN context_campaign_source IS NULL                                         THEN 'Unknown'
                         ELSE 'Others'
                       END::VARCHAR(64)
                FROM javascript_upff_home.pages
                --WHERE path IN ('/stream/', '/subscribe/')
            ),

      sign_up_viewed_events AS (
      SELECT 'iOS'::VARCHAR(32) AS platform, anonymous_id, received_at FROM ios.sign_up_viewed
      UNION ALL
      SELECT 'Android' AS platform, anonymous_id, received_at FROM android.sign_up_viewed
      UNION ALL
      SELECT 'Roku'    AS platform, anonymous_id, received_at FROM roku.sign_up_viewed
      UNION ALL
      SELECT 'Amazon Fire TV' AS platform, anonymous_id, received_at FROM amazon_fire_tv.sign_up_viewed
      UNION ALL
      SELECT 'Tizen TV' AS platform, anonymous_id, received_at FROM tizen_tv.sign_up_viewed
      UNION ALL
      SELECT 'Web'     AS platform, anonymous_id, received_at
      FROM javascript_upentertainment_checkout.product_viewed
      WHERE brand = 'upfaithandfamily'
      ),

      plan_chosen_events AS (
      SELECT 'iOS'::VARCHAR(32) AS platform, anonymous_id, received_at FROM ios.subscription_plan_chosen
      UNION ALL
      SELECT 'Android' AS platform, anonymous_id, received_at FROM android.subscription_plan_chosen
      UNION ALL
      SELECT 'Roku'    AS platform, anonymous_id, received_at FROM roku.subscription_plan_chosen
      UNION ALL
      SELECT 'Amazon Fire TV' AS platform, anonymous_id, received_at FROM amazon_fire_tv.subscription_plan_chosen
      UNION ALL
      SELECT 'Tizen TV' AS platform, anonymous_id, received_at FROM tizen_tv.subscription_plan_chosen
      UNION ALL
      SELECT 'Web'     AS platform, anonymous_id, received_at
      FROM javascript_upentertainment_checkout.signed_up
      WHERE brand = 'upfaithandfamily'
      ),

      order_completed_events AS (
      SELECT 'iOS'::VARCHAR(32) AS platform, anonymous_id, received_at FROM ios.order_completed
      UNION ALL
      SELECT 'Android' AS platform, anonymous_id, received_at FROM android.order_completed
      UNION ALL
      SELECT 'Roku'    AS platform, anonymous_id, received_at FROM roku.order_completed
      UNION ALL
      SELECT 'Amazon Fire TV' AS platform, anonymous_id, received_at FROM amazon_fire_tv.order_completed
      UNION ALL
      SELECT 'Tizen TV' AS platform, anonymous_id, received_at FROM tizen_tv.order_completed
      UNION ALL
      SELECT 'Web'     AS platform, anonymous_id, received_at
      FROM javascript_upentertainment_checkout.order_completed
      WHERE brand = 'upfaithandfamily'
      ),

      -- ---------- Step 1: first entry per user per platform in each period ----------
      -- The first entry row is kept whole so its UTM fields describe how the
      -- user arrived (first touch within the period).
      entries AS (
      SELECT platform, anonymous_id, entry_at, period,
      campaign_source, campaign_name, campaign_medium, campaign_content, campaign_term, marketing_platform
      FROM (
      SELECT platform, anonymous_id, received_at AS entry_at, 'Current' AS period,
      campaign_source, campaign_name, campaign_medium, campaign_content, campaign_term, marketing_platform,
      ROW_NUMBER() OVER (PARTITION BY platform, anonymous_id ORDER BY received_at) AS rn
      FROM entry_events
      WHERE {% condition current_period %} received_at {% endcondition %}

      UNION ALL

      SELECT platform, anonymous_id, received_at AS entry_at, 'Prior' AS period,
      campaign_source, campaign_name, campaign_medium, campaign_content, campaign_term, marketing_platform,
      ROW_NUMBER() OVER (PARTITION BY platform, anonymous_id ORDER BY received_at) AS rn
      FROM entry_events
      WHERE {% condition prior_period %} received_at {% endcondition %}
      ) ranked
      WHERE rn = 1
      -- Web Campaign Filters: applied to Web users only; app users always pass
      AND (
      platform <> 'Web'
      OR (    {% condition marketing_platform_filter %} marketing_platform {% endcondition %}
      AND {% condition campaign_source_filter %}    campaign_source    {% endcondition %}
      AND {% condition campaign_name_filter %}      campaign_name      {% endcondition %}
      AND {% condition campaign_medium_filter %}    campaign_medium    {% endcondition %}
      )
      )
      ),

      -- ---------- Step 2 ----------
      signup_viewed AS (
      SELECT en.platform, en.anonymous_id, en.period, MIN(e.received_at) AS signup_viewed_at
      FROM entries en
      JOIN sign_up_viewed_events e
      ON  e.anonymous_id = en.anonymous_id
      AND e.platform     = en.platform
      AND e.received_at >= en.entry_at
      AND e.received_at <  DATEADD(day, {% parameter attribution_days %}, en.entry_at)
      GROUP BY en.platform, en.anonymous_id, en.period
      ),

      -- ---------- Step 3 ----------
      plan_chosen AS (
      SELECT sv.platform, sv.anonymous_id, sv.period, MIN(e.received_at) AS plan_chosen_at
      FROM signup_viewed sv
      JOIN entries en
      ON en.anonymous_id = sv.anonymous_id AND en.platform = sv.platform AND en.period = sv.period
      JOIN plan_chosen_events e
      ON  e.anonymous_id = sv.anonymous_id
      AND e.platform     = sv.platform
      AND e.received_at >= sv.signup_viewed_at
      AND e.received_at <  DATEADD(day, {% parameter attribution_days %}, en.entry_at)
      GROUP BY sv.platform, sv.anonymous_id, sv.period
      ),

      -- ---------- Step 4 ----------
      order_completed AS (
      SELECT pc.platform, pc.anonymous_id, pc.period, MIN(e.received_at) AS order_completed_at
      FROM plan_chosen pc
      JOIN entries en
      ON en.anonymous_id = pc.anonymous_id AND en.platform = pc.platform AND en.period = pc.period
      JOIN order_completed_events e
      ON  e.anonymous_id = pc.anonymous_id
      AND e.platform     = pc.platform
      AND e.received_at >= pc.plan_chosen_at
      AND e.received_at <  DATEADD(day, {% parameter attribution_days %}, en.entry_at)
      GROUP BY pc.platform, pc.anonymous_id, pc.period
      ),

      user_funnel AS (
      SELECT
      en.platform
      , CASE WHEN en.platform = 'Web'                                   THEN 'Web'
      WHEN en.platform IN ('Roku', 'Amazon Fire TV', 'Tizen TV')     THEN 'Connected TV'
      ELSE 'Mobile App' END AS platform_group
      , en.anonymous_id
      , en.period
      , en.entry_at
      , DATE_TRUNC('day', en.entry_at) AS entry_day
      , en.campaign_source
      , en.campaign_name
      , en.campaign_medium
      , en.campaign_content
      , en.campaign_term
      , en.marketing_platform
      , sv.signup_viewed_at
      , pc.plan_chosen_at
      , oc.order_completed_at
      FROM entries en
      LEFT JOIN signup_viewed sv
      ON sv.anonymous_id = en.anonymous_id AND sv.platform = en.platform AND sv.period = en.period
      LEFT JOIN plan_chosen pc
      ON pc.anonymous_id = en.anonymous_id AND pc.platform = en.platform AND pc.period = en.period
      LEFT JOIN order_completed oc
      ON oc.anonymous_id = en.anonymous_id AND oc.platform = en.platform AND oc.period = en.period
      ),

      user_days AS (
      SELECT
      uf.anonymous_id || '|' || uf.platform || '|' || uf.period AS user_pk
      , uf.*
      -- Day number within the period (1 = first entry day), shared by all platforms
      , DATEDIFF(day, MIN(uf.entry_day) OVER (PARTITION BY uf.period), uf.entry_day) + 1 AS day_of_period

      -- Same-day counts for the average daily rates, at three levels:
      --   _all = all platforms, _group = Mobile App / Connected TV / Web, _platform = each platform
      , COUNT(*)                   OVER (PARTITION BY uf.period, uf.entry_day)                    AS day_entries_all
      , COUNT(*)                   OVER (PARTITION BY uf.period, uf.platform_group, uf.entry_day) AS day_entries_group
      , COUNT(*)                   OVER (PARTITION BY uf.period, uf.platform, uf.entry_day)       AS day_entries_platform
      , COUNT(uf.signup_viewed_at) OVER (PARTITION BY uf.period, uf.entry_day)                    AS day_signup_viewed_all
      , COUNT(uf.signup_viewed_at) OVER (PARTITION BY uf.period, uf.platform_group, uf.entry_day) AS day_signup_viewed_group
      , COUNT(uf.signup_viewed_at) OVER (PARTITION BY uf.period, uf.platform, uf.entry_day)       AS day_signup_viewed_platform
      , COUNT(uf.plan_chosen_at)   OVER (PARTITION BY uf.period, uf.entry_day)                    AS day_plan_chosen_all
      , COUNT(uf.plan_chosen_at)   OVER (PARTITION BY uf.period, uf.platform_group, uf.entry_day) AS day_plan_chosen_group
      , COUNT(uf.plan_chosen_at)   OVER (PARTITION BY uf.period, uf.platform, uf.entry_day)       AS day_plan_chosen_platform
      FROM user_funnel uf
      ),

      -- One row per funnel step + OVERALL, so steps can be rows in a table or chart
      steps AS (
      SELECT 1 AS step_number, 'App Installed / Landing Page Visit' AS step_name UNION ALL
      SELECT 2, 'Sign Up Viewed / Product Viewed'                               UNION ALL
      SELECT 3, 'Plan Chosen / Signed Up'                                       UNION ALL
      SELECT 4, 'Order Completed'                                               UNION ALL
      SELECT 5, 'OVERALL: Entry -> Order'
      )

      SELECT
      ud.user_pk || '|' || CAST(s.step_number AS VARCHAR) AS pk
      , ud.*
      , s.step_number
      , s.step_name
      -- Did this user reach this step?
      , CASE s.step_number
      WHEN 1 THEN 1
      WHEN 2 THEN CASE WHEN ud.signup_viewed_at   IS NOT NULL THEN 1 ELSE 0 END
      WHEN 3 THEN CASE WHEN ud.plan_chosen_at     IS NOT NULL THEN 1 ELSE 0 END
      ELSE        CASE WHEN ud.order_completed_at IS NOT NULL THEN 1 ELSE 0 END
      END AS reached_flag
      -- Did this user reach the previous step? (steps 2-4 only)
      , CASE s.step_number
      WHEN 2 THEN 1
      WHEN 3 THEN CASE WHEN ud.signup_viewed_at IS NOT NULL THEN 1 ELSE 0 END
      WHEN 4 THEN CASE WHEN ud.plan_chosen_at   IS NOT NULL THEN 1 ELSE 0 END
      ELSE 0
      END AS prev_flag
      -- Hours from the previous step (OVERALL = entry -> order)
      , CASE s.step_number
      WHEN 2 THEN DATEDIFF(minute, ud.entry_at,         ud.signup_viewed_at)   / 60.0
      WHEN 3 THEN DATEDIFF(minute, ud.signup_viewed_at, ud.plan_chosen_at)     / 60.0
      WHEN 4 THEN DATEDIFF(minute, ud.plan_chosen_at,   ud.order_completed_at) / 60.0
      WHEN 5 THEN DATEDIFF(minute, ud.entry_at,         ud.order_completed_at) / 60.0
      END AS hours_from_prev
      FROM user_days ud
      CROSS JOIN steps s
      ;;
  }

  # ---------------------------------------------------------------------------
  # Business dimensions
  # ---------------------------------------------------------------------------

  dimension: platform {
    label: "Platform"
    description: "Where the user entered the funnel: iOS app, Android app, a Connected TV app (Roku, Amazon Fire TV, Tizen TV), or Web (upfaithandfamily.com marketing site). Use to filter or break down any metric by platform. Also called: device, channel, app vs web, CTV."
    type: string
    sql: ${TABLE}.platform ;;
    suggestions: ["iOS", "Android", "Roku", "Amazon Fire TV", "Tizen TV", "Web"]
  }

  dimension: platform_group {
    label: "Platform Group"
    description: "Mobile App (iOS and Android combined), Connected TV (Roku, Amazon Fire TV and Tizen TV combined) or Web. Use when the question is about mobile, TV apps or the website overall. Also called: app vs web, mobile vs TV vs web, CTV, OTT, smart TV."
    type: string
    sql: ${TABLE}.platform_group ;;
    suggestions: ["Mobile App", "Connected TV", "Web"]
  }

  dimension: period {
    label: "Period"
    description: "Current or Prior, based on the Current Period and Prior Period filters. Pivot or group by this to compare any metric between the two periods."
    type: string
    sql: ${TABLE}.period ;;
    order_by_field: period_sort
    suggestions: ["Current", "Prior"]
  }

  dimension: step_number {
    label: "Funnel Step Number"
    description: "Order of the funnel step: 1 Entry, 2 Sign Up Viewed / Product Viewed, 3 Plan Chosen / Signed Up, 4 Order Completed, 5 OVERALL (entry to order)."
    type: number
    sql: ${TABLE}.step_number ;;
    value_format_name: id
  }

  dimension: step_name {
    label: "Funnel Step"
    description: "Name of the funnel step. Required for all 'Funnel Step' metrics. Steps: App Installed / Landing Page Visit, Sign Up Viewed / Product Viewed, Plan Chosen / Signed Up, Order Completed, and OVERALL: Entry -> Order. Also called: stage, funnel stage, checkout step."
    type: string
    sql: ${TABLE}.step_name ;;
    order_by_field: step_number
    suggestions: ["App Installed / Landing Page Visit", "Sign Up Viewed / Product Viewed",
      "Plan Chosen / Signed Up", "Order Completed", "OVERALL: Entry -> Order"]
  }

  dimension_group: entry {
    label: "Entry"
    description: "When the user entered the funnel: app install time (iOS, Android, Connected TV) or first landing page visit (Web). Use for daily or weekly trends. Also called: install date, visit date, sign-up start date."
    type: time
    timeframes: [raw, time, date, week, month, day_of_week]
    sql: ${TABLE}.entry_at ;;
  }

  dimension: day_of_period {
    label: "Day of Period"
    description: "Day number within the period, where 1 is the first day. Use as the x-axis to overlay Current and Prior day by day."
    type: number
    sql: ${TABLE}.day_of_period ;;
    value_format_name: id
  }

  dimension: converted {
    label: "Converted"
    description: "Yes if the user completed an order (subscription purchase) within the attribution window. Also called: subscribed, purchased, converted user."
    type: yesno
    sql: ${TABLE}.order_completed_at IS NOT NULL ;;
  }

  dimension: anonymous_id {
    label: "Anonymous ID"
    description: "Segment anonymous ID of the user. Use only for user-level drill-downs."
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  # ---------------------------------------------------------------------------
  # Web campaign (UTM) dimensions - for grouping and breakdowns
  # From the user's first marketing-site page view in the period (first touch).
  # can_filter: no, so filtering always goes through the Web Campaign Filters
  # above, which keep iOS, Android and Connected TV users in the results.
  # ---------------------------------------------------------------------------

  dimension: campaign_source {
    group_label: "Web Campaign (UTM)"
    label: "Campaign Source"
    description: "utm_source of the visit that brought the user to the marketing site (Segment context_campaign_source), e.g. google, facebook, hubspot. Web only (mobile and CTV app users are blank). Group by this; to filter, use the Web Campaign Filters. Also called: UTM source, traffic source, referrer source."
    type: string
    sql: ${TABLE}.campaign_source ;;
    can_filter: no
  }

  dimension: campaign_name {
    group_label: "Web Campaign (UTM)"
    label: "Campaign Name"
    description: "utm_campaign of the visit that brought the user to the marketing site (Segment context_campaign_name). Web only (mobile and CTV app users are blank). Group by this; to filter, use the Web Campaign Filters. Also called: UTM campaign, campaign, ad campaign."
    type: string
    sql: ${TABLE}.campaign_name ;;
    can_filter: no
  }

  dimension: campaign_medium {
    group_label: "Web Campaign (UTM)"
    label: "Campaign Medium"
    description: "utm_medium of the arriving visit (Segment context_campaign_medium), e.g. cpc, email, social. Web only (mobile and CTV app users are blank). Group by this; to filter, use the Web Campaign Filters. Also called: UTM medium, channel type."
    type: string
    sql: ${TABLE}.campaign_medium ;;
    can_filter: no
  }

  dimension: campaign_content {
    group_label: "Web Campaign (UTM)"
    label: "Campaign Content"
    description: "utm_content of the arriving visit (Segment context_campaign_content), usually the ad or creative variant. Web only (mobile and CTV app users are blank). Group by this; to filter, use the Web Campaign Filters. Also called: UTM content, ad variant, creative."
    type: string
    sql: ${TABLE}.campaign_content ;;
    can_filter: no
  }

  dimension: campaign_term {
    group_label: "Web Campaign (UTM)"
    label: "Campaign Term"
    description: "utm_term of the arriving visit (Segment context_campaign_term), usually the paid search keyword. Web only (mobile and CTV app users are blank). Group by this; to filter, use the Web Campaign Filters. Also called: UTM term, keyword."
    type: string
    sql: ${TABLE}.campaign_term ;;
    can_filter: no
  }

  dimension: marketing_platform {
    group_label: "Web Campaign (UTM)"
    label: "Marketing Platform"
    description: "Normalized marketing platform for the visit that brought a web user to the site, based on Campaign Source, Medium and Name: Google Search, Google PMax, Google Display, YouTube, Meta Ads, Bing Ads, HubSpot, UPtv Digital, ChatGPT, Organic Search, Organic Social, Others, Unknown. iOS and Android users show as Mobile App (no UTM); Connected TV users show as Connected TV (no UTM). Group by this; to filter, use the Marketing Platform Filter. Also called: channel, ad platform, traffic channel."
    type: string
    sql: ${TABLE}.marketing_platform ;;
    can_filter: no
  }

  # ---------------------------------------------------------------------------
  # Hidden helper fields (not shown to users or to Conversational Analytics)
  # ---------------------------------------------------------------------------

  dimension: pk {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.pk ;;
  }

  dimension: user_pk {
    hidden: yes
    type: string
    sql: ${TABLE}.user_pk ;;
  }

  dimension: period_sort {
    hidden: yes
    type: number
    sql: CASE WHEN ${TABLE}.period = 'Prior' THEN 1 ELSE 2 END ;;
  }

  dimension_group: signup_viewed {
    hidden: yes
    type: time
    timeframes: [raw, time]
    sql: ${TABLE}.signup_viewed_at ;;
  }

  dimension_group: plan_chosen {
    hidden: yes
    type: time
    timeframes: [raw, time]
    sql: ${TABLE}.plan_chosen_at ;;
  }

  dimension_group: order_completed {
    hidden: yes
    type: time
    timeframes: [raw, time]
    sql: ${TABLE}.order_completed_at ;;
  }

  dimension: reached_flag {
    hidden: yes
    type: number
    sql: ${TABLE}.reached_flag ;;
  }

  dimension: prev_flag {
    hidden: yes
    type: number
    sql: ${TABLE}.prev_flag ;;
  }

  dimension: hours_from_prev {
    hidden: yes
    type: number
    sql: ${TABLE}.hours_from_prev ;;
  }

  # Average daily helpers: pick the day-level count that matches how the query
  # is sliced (Platform > Platform Group > all platforms).
  dimension: day_entries {
    hidden: yes
    type: number
    sql:
      {% if upff_signup_funnel.platform._is_selected or upff_signup_funnel.platform._is_filtered %} ${TABLE}.day_entries_platform
      {% elsif upff_signup_funnel.platform_group._is_selected or upff_signup_funnel.platform_group._is_filtered %} ${TABLE}.day_entries_group
      {% else %} ${TABLE}.day_entries_all {% endif %} ;;
  }

  dimension: day_signup_viewed {
    hidden: yes
    type: number
    sql:
      {% if upff_signup_funnel.platform._is_selected or upff_signup_funnel.platform._is_filtered %} ${TABLE}.day_signup_viewed_platform
      {% elsif upff_signup_funnel.platform_group._is_selected or upff_signup_funnel.platform_group._is_filtered %} ${TABLE}.day_signup_viewed_group
      {% else %} ${TABLE}.day_signup_viewed_all {% endif %} ;;
  }

  dimension: day_plan_chosen {
    hidden: yes
    type: number
    sql:
      {% if upff_signup_funnel.platform._is_selected or upff_signup_funnel.platform._is_filtered %} ${TABLE}.day_plan_chosen_platform
      {% elsif upff_signup_funnel.platform_group._is_selected or upff_signup_funnel.platform_group._is_filtered %} ${TABLE}.day_plan_chosen_group
      {% else %} ${TABLE}.day_plan_chosen_all {% endif %} ;;
  }

  dimension: day_prev_count {
    hidden: yes
    type: number
    sql:
      CASE ${TABLE}.step_number
        WHEN 2 THEN ${day_entries}
        WHEN 3 THEN ${day_signup_viewed}
        WHEN 4 THEN ${day_plan_chosen}
      END ;;
  }

  dimension: day_key {
    hidden: yes
    type: string
    sql:
      {% if upff_signup_funnel.platform._is_selected or upff_signup_funnel.platform._is_filtered %}
        ${TABLE}.period || '|' || ${TABLE}.platform || '|' || CAST(${TABLE}.entry_day AS VARCHAR)
      {% elsif upff_signup_funnel.platform_group._is_selected or upff_signup_funnel.platform_group._is_filtered %}
        ${TABLE}.period || '|' || ${TABLE}.platform_group || '|' || CAST(${TABLE}.entry_day AS VARCHAR)
      {% else %}
        ${TABLE}.period || '|' || CAST(${TABLE}.entry_day AS VARCHAR)
      {% endif %} ;;
  }

  # ---------------------------------------------------------------------------
  # Headline metrics - Current period (no Funnel Step needed)
  # ---------------------------------------------------------------------------

  measure: entries_current {
    group_label: "Headline Metrics"
    label: "Entries (Current Period)"
    description: "Number of users who entered the funnel in the current period: app installs (iOS, Android, Connected TV) plus landing page visitors (Web). Also called: installs, visitors, traffic, top of funnel."
    type: count_distinct
    sql: ${user_pk} ;;
    filters: [period: "Current"]
    value_format_name: decimal_0
  }

  measure: conversions_current {
    group_label: "Headline Metrics"
    label: "Conversions (Current Period)"
    description: "Number of users from the current period who completed an order within the attribution window. Also called: sign-ups, subscriptions, orders, purchases, new subscribers."
    type: count_distinct
    sql: ${user_pk} ;;
    filters: [period: "Current", converted: "yes"]
    value_format_name: decimal_0
  }

  measure: effective_conversion_current {
    group_label: "Headline Metrics"
    label: "Conversion Rate (Current Period)"
    description: "Effective entry-to-order conversion rate for the current period: conversions divided by entries (pooled across all days). For Web this is marketing-site visit to order. This is the main conversion rate. Also called: effective conversion rate, overall conversion rate, sign-up rate, install-to-subscription rate."
    type: number
    sql: 1.0 * ${conversions_current} / NULLIF(${entries_current}, 0) ;;
    value_format_name: percent_2
  }

  measure: avg_daily_effective_conversion_current {
    group_label: "Headline Metrics"
    label: "Average Daily Conversion Rate (Current Period)"
    description: "Average of each day's entry-to-order conversion rate in the current period, with every day weighted equally. Use when asked for a typical day or the average daily rate; otherwise prefer Conversion Rate (Current Period)."
    type: number
    sql:
      SUM(CASE WHEN ${TABLE}.step_number = 1 AND ${TABLE}.period = 'Current' AND ${TABLE}.order_completed_at IS NOT NULL
               THEN 1.0 / ${day_entries} ELSE 0 END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.step_number = 1 AND ${TABLE}.period = 'Current' THEN ${day_key} END), 0) ;;
    value_format_name: percent_2
  }

  # ---------------------------------------------------------------------------
  # Headline metrics - Prior period (no Funnel Step needed)
  # ---------------------------------------------------------------------------

  measure: entries_prior {
    group_label: "Headline Metrics"
    label: "Entries (Prior Period)"
    description: "Number of users who entered the funnel in the prior period: app installs (iOS, Android, Connected TV) plus landing page visitors (Web). Also called: installs, visitors, traffic, top of funnel."
    type: count_distinct
    sql: ${user_pk} ;;
    filters: [period: "Prior"]
    value_format_name: decimal_0
  }

  measure: conversions_prior {
    group_label: "Headline Metrics"
    label: "Conversions (Prior Period)"
    description: "Number of users from the prior period who completed an order within the attribution window. Also called: sign-ups, subscriptions, orders, purchases, new subscribers."
    type: count_distinct
    sql: ${user_pk} ;;
    filters: [period: "Prior", converted: "yes"]
    value_format_name: decimal_0
  }

  measure: effective_conversion_prior {
    group_label: "Headline Metrics"
    label: "Conversion Rate (Prior Period)"
    description: "Effective entry-to-order conversion rate for the prior period: conversions divided by entries (pooled across all days). For Web this is marketing-site visit to order. This is the main conversion rate. Also called: effective conversion rate, overall conversion rate, sign-up rate, install-to-subscription rate."
    type: number
    sql: 1.0 * ${conversions_prior} / NULLIF(${entries_prior}, 0) ;;
    value_format_name: percent_2
  }

  measure: avg_daily_effective_conversion_prior {
    group_label: "Headline Metrics"
    label: "Average Daily Conversion Rate (Prior Period)"
    description: "Average of each day's entry-to-order conversion rate in the prior period, with every day weighted equally. Use when asked for a typical day or the average daily rate; otherwise prefer Conversion Rate (Prior Period)."
    type: number
    sql:
      SUM(CASE WHEN ${TABLE}.step_number = 1 AND ${TABLE}.period = 'Prior' AND ${TABLE}.order_completed_at IS NOT NULL
               THEN 1.0 / ${day_entries} ELSE 0 END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.step_number = 1 AND ${TABLE}.period = 'Prior' THEN ${day_key} END), 0) ;;
    value_format_name: percent_2
  }

  measure: conversions_pct_change {
    group_label: "Headline Metrics"
    label: "Conversions Percent Change (Current vs Prior)"
    description: "Percent change in conversions from the prior period to the current period. Also called: growth in sign-ups, change in subscriptions."
    type: number
    sql: (1.0 * ${conversions_current} - ${conversions_prior}) / NULLIF(${conversions_prior}, 0) ;;
    value_format_name: percent_2
  }

  measure: effective_conversion_change_pp {
    group_label: "Headline Metrics"
    label: "Conversion Rate Change in Percentage Points (Current vs Prior)"
    description: "Current period conversion rate minus prior period conversion rate, in percentage points (e.g. 1.5 means the rate rose by 1.5 points). Use when asked whether conversion improved or declined."
    type: number
    sql: 100.0 * (${effective_conversion_current} - ${effective_conversion_prior}) ;;
    value_format_name: decimal_2
  }

  measure: avg_daily_effective_conversion_change_pp {
    group_label: "Headline Metrics"
    label: "Average Daily Conversion Rate Change in Percentage Points"
    description: "Current minus prior average daily conversion rate, in percentage points."
    type: number
    sql: 100.0 * (${avg_daily_effective_conversion_current} - ${avg_daily_effective_conversion_prior}) ;;
    value_format_name: decimal_2
  }

  # ---------------------------------------------------------------------------
  # Trend metrics (group by Period with Entry Date or Day of Period)
  # ---------------------------------------------------------------------------

  measure: entries {
    group_label: "Trend Metrics"
    label: "Entries"
    description: "Number of users who entered the funnel (installs plus landing page visitors). Group by Period, Entry Date or Day of Period for trends."
    type: count_distinct
    sql: ${user_pk} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
  }

  measure: conversions {
    group_label: "Trend Metrics"
    label: "Conversions"
    description: "Number of users who completed an order within the attribution window. Group by Period, Entry Date or Day of Period for trends. Also called: sign-ups, subscriptions, orders."
    type: count_distinct
    sql: ${user_pk} ;;
    filters: [converted: "yes"]
    value_format_name: decimal_0
    drill_fields: [detail*]
  }

  measure: effective_conversion {
    group_label: "Trend Metrics"
    label: "Conversion Rate"
    description: "Entry-to-order conversion rate (conversions / entries). Group by Period to compare Current vs Prior, or by Entry Date / Day of Period for a daily trend."
    type: number
    sql: 1.0 * ${conversions} / NULLIF(${entries}, 0) ;;
    value_format_name: percent_2
  }

  # ---------------------------------------------------------------------------
  # Platform comparison (respects the period filters)
  # ---------------------------------------------------------------------------

  measure: effective_conversion_ios {
    group_label: "Platform Comparison"
    label: "Conversion Rate - iOS"
    description: "Entry-to-order conversion rate for the iOS app only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'iOS' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'iOS' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }

  measure: effective_conversion_android {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Android"
    description: "Entry-to-order conversion rate for the Android app only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Android' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'Android' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }
  measure: effective_conversion_roku {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Roku"
    description: "Entry-to-order conversion rate for the Roku app only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Roku' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'Roku' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }

  measure: effective_conversion_fire_tv {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Amazon Fire TV"
    description: "Entry-to-order conversion rate for the Amazon Fire TV app only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Amazon Fire TV' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'Amazon Fire TV' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }

  measure: effective_conversion_tizen {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Tizen TV"
    description: "Entry-to-order conversion rate for the Tizen TV (Samsung) app only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Tizen TV' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'Tizen TV' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }

  measure: effective_conversion_ctv {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Connected TV"
    description: "Entry-to-order conversion rate for all Connected TV apps combined (Roku, Amazon Fire TV, Tizen TV). Also called: CTV conversion, TV app conversion."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform_group} = 'Connected TV' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform_group} = 'Connected TV' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }


  measure: effective_conversion_web {
    group_label: "Platform Comparison"
    label: "Conversion Rate - Web"
    description: "Marketing-site landing page visit to order conversion rate for Web only."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Web' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${platform} = 'Web' THEN ${user_pk} END), 0) ;;
    value_format_name: percent_2
  }

  measure: web_share_of_conversions {
    group_label: "Platform Comparison"
    label: "Web Share of Conversions"
    description: "Percent of all conversions that came from Web (vs mobile and Connected TV apps)."
    type: number
    sql: 1.0 * COUNT(DISTINCT CASE WHEN ${platform} = 'Web' AND ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.order_completed_at IS NOT NULL THEN ${user_pk} END), 0) ;;
    value_format_name: percent_1
  }

  # ---------------------------------------------------------------------------
  # Funnel step metrics - Prior period (require Funnel Step)
  # ---------------------------------------------------------------------------

  measure: users_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Users at Step (Prior Period)"
    description: "Number of prior-period users who reached each funnel step. Use with Funnel Step. Also called: step volume, users per stage."
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Prior' AND ${reached_flag} = 1 THEN ${user_pk} END) ;;
    value_format_name: decimal_0
    required_fields: [step_name]
  }

  measure: prev_users_prior {
    hidden: yes
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Prior' AND ${prev_flag} = 1 THEN ${user_pk} END) ;;
  }

  measure: step_entries_prior {
    hidden: yes
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Prior' THEN ${user_pk} END) ;;
  }

  measure: pct_of_entries_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Percent of Entries Reaching Step (Prior Period)"
    description: "Share of all prior-period entries that reached each step (effective, cumulative conversion from the top of the funnel). On the OVERALL row this equals the entry-to-order conversion rate. Use with Funnel Step."
    type: number
    sql: 1.0 * ${users_prior} / NULLIF(${step_entries_prior}, 0) ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: step_conversion_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Step Conversion Rate (Prior Period)"
    description: "Share of users at the previous step who reached this step, for steps 2-4 (prior period). Use to find where users drop off. Also called: step-to-step conversion, drop-off rate (100% minus this). Use with Funnel Step."
    type: number
    sql: CASE WHEN MAX(${step_number}) BETWEEN 2 AND 4
      THEN 1.0 * ${users_prior} / NULLIF(${prev_users_prior}, 0) END ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_daily_step_conversion_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Average Daily Step Conversion Rate (Prior Period)"
    description: "Step conversion rate from the previous step, averaged across each day of the prior period (steps 2-4). Use with Funnel Step."
    type: number
    sql:
      CASE WHEN MAX(${step_number}) BETWEEN 2 AND 4 THEN
        SUM(CASE WHEN ${TABLE}.period = 'Prior' AND ${reached_flag} = 1
                 THEN 1.0 / NULLIF(${day_prev_count}, 0) ELSE 0 END)
        / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Prior' AND ${prev_flag} = 1 THEN ${day_key} END), 0)
      END ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_daily_effective_conversion_step_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Average Daily Percent of Entries Reaching Step (Prior Period)"
    description: "Share of entries reaching each step, averaged across each day of the prior period. On the OVERALL row this is the average daily entry-to-order conversion rate. Use with Funnel Step."
    type: number
    sql:
      SUM(CASE WHEN ${TABLE}.period = 'Prior' AND ${reached_flag} = 1
               THEN 1.0 / ${day_entries} ELSE 0 END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Prior' THEN ${day_key} END), 0) ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_hrs_from_prev_step_prior {
    group_label: "Funnel Step Metrics - Prior"
    label: "Average Hours from Previous Step (Prior Period)"
    description: "Average hours users took to reach each step from the previous step (prior period). On the OVERALL row, average hours from entry to order. Also called: time to convert, time between steps. Use with Funnel Step."
    type: number
    sql: AVG(CASE WHEN ${TABLE}.period = 'Prior' AND ${reached_flag} = 1 THEN ${hours_from_prev} END) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  # ---------------------------------------------------------------------------
  # Funnel step metrics - Current period (require Funnel Step)
  # ---------------------------------------------------------------------------

  measure: users_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Users at Step (Current Period)"
    description: "Number of current-period users who reached each funnel step. Use with Funnel Step. Also called: step volume, users per stage."
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Current' AND ${reached_flag} = 1 THEN ${user_pk} END) ;;
    value_format_name: decimal_0
    required_fields: [step_name]
  }

  measure: prev_users_current {
    hidden: yes
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Current' AND ${prev_flag} = 1 THEN ${user_pk} END) ;;
  }

  measure: step_entries_current {
    hidden: yes
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Current' THEN ${user_pk} END) ;;
  }

  measure: pct_of_entries_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Percent of Entries Reaching Step (Current Period)"
    description: "Share of all current-period entries that reached each step (effective, cumulative conversion from the top of the funnel). On the OVERALL row this equals the entry-to-order conversion rate. Use with Funnel Step."
    type: number
    sql: 1.0 * ${users_current} / NULLIF(${step_entries_current}, 0) ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: step_conversion_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Step Conversion Rate (Current Period)"
    description: "Share of users at the previous step who reached this step, for steps 2-4 (current period). Use to find where users drop off. Also called: step-to-step conversion, drop-off rate (100% minus this). Use with Funnel Step."
    type: number
    sql: CASE WHEN MAX(${step_number}) BETWEEN 2 AND 4
      THEN 1.0 * ${users_current} / NULLIF(${prev_users_current}, 0) END ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_daily_step_conversion_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Average Daily Step Conversion Rate (Current Period)"
    description: "Step conversion rate from the previous step, averaged across each day of the current period (steps 2-4). Use with Funnel Step."
    type: number
    sql:
      CASE WHEN MAX(${step_number}) BETWEEN 2 AND 4 THEN
        SUM(CASE WHEN ${TABLE}.period = 'Current' AND ${reached_flag} = 1
                 THEN 1.0 / NULLIF(${day_prev_count}, 0) ELSE 0 END)
        / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Current' AND ${prev_flag} = 1 THEN ${day_key} END), 0)
      END ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_daily_effective_conversion_step_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Average Daily Percent of Entries Reaching Step (Current Period)"
    description: "Share of entries reaching each step, averaged across each day of the current period. On the OVERALL row this is the average daily entry-to-order conversion rate. Use with Funnel Step."
    type: number
    sql:
      SUM(CASE WHEN ${TABLE}.period = 'Current' AND ${reached_flag} = 1
               THEN 1.0 / ${day_entries} ELSE 0 END)
      / NULLIF(COUNT(DISTINCT CASE WHEN ${TABLE}.period = 'Current' THEN ${day_key} END), 0) ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: avg_hrs_from_prev_step_current {
    group_label: "Funnel Step Metrics - Current"
    label: "Average Hours from Previous Step (Current Period)"
    description: "Average hours users took to reach each step from the previous step (current period). On the OVERALL row, average hours from entry to order. Also called: time to convert, time between steps. Use with Funnel Step."
    type: number
    sql: AVG(CASE WHEN ${TABLE}.period = 'Current' AND ${reached_flag} = 1 THEN ${hours_from_prev} END) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  # ---------------------------------------------------------------------------
  # Funnel step metrics - change, Current vs Prior (require Funnel Step)
  # ---------------------------------------------------------------------------

  measure: users_pct_change {
    group_label: "Funnel Step Metrics - Change"
    label: "Users at Step Percent Change"
    description: "Percent change in users reaching each step, current vs prior period. Use with Funnel Step."
    type: number
    sql: (1.0 * ${users_current} - ${users_prior}) / NULLIF(${users_prior}, 0) ;;
    value_format_name: percent_2
    required_fields: [step_name]
  }

  measure: pct_of_entries_change_pp {
    group_label: "Funnel Step Metrics - Change"
    label: "Percent of Entries Reaching Step Change in Percentage Points"
    description: "Current minus prior share of entries reaching each step, in percentage points. On the OVERALL row, the change in entry-to-order conversion rate. Use with Funnel Step."
    type: number
    sql: 100.0 * (${pct_of_entries_current} - ${pct_of_entries_prior}) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  measure: step_conversion_change_pp {
    group_label: "Funnel Step Metrics - Change"
    label: "Step Conversion Rate Change in Percentage Points"
    description: "Current minus prior step conversion rate, in percentage points. Use to find which step improved or declined most. Use with Funnel Step."
    type: number
    sql: 100.0 * (${step_conversion_current} - ${step_conversion_prior}) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  measure: avg_daily_step_conversion_change_pp {
    group_label: "Funnel Step Metrics - Change"
    label: "Average Daily Step Conversion Rate Change in Percentage Points"
    description: "Current minus prior average daily step conversion rate, in percentage points. Use with Funnel Step."
    type: number
    sql: 100.0 * (${avg_daily_step_conversion_current} - ${avg_daily_step_conversion_prior}) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  measure: avg_daily_effective_conversion_step_change_pp {
    group_label: "Funnel Step Metrics - Change"
    label: "Average Daily Percent of Entries Reaching Step Change in Percentage Points"
    description: "Current minus prior average daily share of entries reaching each step, in percentage points. Use with Funnel Step."
    type: number
    sql: 100.0 * (${avg_daily_effective_conversion_step_current} - ${avg_daily_effective_conversion_step_prior}) ;;
    value_format_name: decimal_2
    required_fields: [step_name]
  }

  set: detail {
    fields: [platform, anonymous_id, period, entry_time, marketing_platform, campaign_source, campaign_name, converted]
  }
}
