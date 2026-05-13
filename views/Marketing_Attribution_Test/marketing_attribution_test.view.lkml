################################################################################
# View: marketing_attribution_test
#
# === INCREMENTAL PDT ===
# This view is built as an incremental PDT. On each run, Looker appends new
# rows for any report_date >= (max_report_date_in_table - 7 days).
#
# How it works:
#   - increment_key:    report_date
#   - increment_offset: 7  (re-pulls the last 7 days each run, in addition to anything new)
#   - The 7-day look-back handles late-arriving data:
#       * Branch.io can re-emit aggregate rows for past dates as attribution refines
#       * Web page visits can arrive after their associated conversion
#       * Retroactive attribution changes within ~1 week are captured
#
# When to rebuild from scratch:
#   - If late-arriving data extends past 7 days on a particular event
#   - If you change a derived column (credit_weight formula, quality_score weights)
#   - If you change a CTE structure
#   Use Looker's "Rebuild Derived Tables & Run" or persist_with: rebuilds
#
# Trade-offs vs. the prior full-rebuild PDT:
#   + Builds are much faster (minutes vs hours on a 180-day window)
#   + Lower warehouse cost
#   - Data older than 7 days will NOT reflect new touches/attribution
#   - One-time rebuild needed when scoring weights or attribution logic changes
#
# === ROW TYPES (event_type) ===
#   1. 'page_visit'    — every marketing-site page view (web)
#   2. 'conversion'    — attributed free trials and reacquisitions (web)
#   3. 'app_trial'     — app free trials from Branch.io (php.branch_purchase)
#   4. 'app_install'   — app installs from Branch.io (php.branch_install)
#   5. 'app_reinstall' — app reinstalls from Branch.io (php.branch_reinstall)
#################################################################################

view: marketing_attribution_test {
  derived_table: {

    # ============================================================
    # INCREMENTAL PDT CONFIG
    # ============================================================
    increment_key: "report_date"
    increment_offset: 7
    datagroup_trigger: marketing_attribution_daily
    distribution_style: even
    #sortkeys: ["report_date", "event_type"]
    indexes: ["report_date", "event_type", "campaign_source", "user_id"]

    sql:
      WITH params AS (
          SELECT
               -- Initial build covers 180 days; incremental runs are filtered
               -- by Looker's injection of incrementcondition (below).
               (CURRENT_DATE - INTERVAL '90 days')::DATE AS start_date
              ,CURRENT_DATE                               AS end_date
              ,90                  AS max_attribution_window_days
              ,0.40                AS w_activations
              ,0.25                AS w_reacquisitions
              ,0.20                AS w_bundle_trials
              ,0.15                AS w_standard_trials
              ,0.50                AS conv_rate_max
              ,10000               AS cancel_window_seconds
              ,1.0                 AS retention_floor
      ),

      -- ============================================================
      -- USER ACTIVATION VALUE from Chargebee
      -- ============================================================
      user_activation_value AS (
      SELECT
      user_id
      ,activation_value_cents / 100.0  AS activation_value_dollars
      ,CASE
      WHEN activation_value_cents >= 5000 THEN 'yearly'
      WHEN activation_value_cents > 0     THEN 'monthly'
      ELSE                                     'unknown'
      END AS plan_type
      ,CASE WHEN activation_value_cents >= 5000 THEN TRUE ELSE FALSE END AS is_yearly_plan
      FROM (
      SELECT
      user_id
      ,content_subscription_subscription_items_0_unit_price AS activation_value_cents
      ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY received_at ASC) AS rn
      FROM chargebee_webhook_events.subscription_activated
      WHERE DATE(received_at) BETWEEN (SELECT start_date FROM params)
      AND (SELECT end_date   FROM params)
      AND content_subscription_subscription_items LIKE '%UP%'
      AND content_subscription_subscription_items_0_unit_price IS NOT NULL
      AND content_subscription_subscription_items_0_unit_price > 0
      AND user_id IS NOT NULL
      ) ranked
      WHERE rn = 1
      ),

      -- ============================================================
      -- BRANCH.IO APP FREE TRIALS — from php.branch_purchase
      -- incrementcondition report_date endincrementcondition
      -- becomes a WHERE clause that Looker injects on incremental runs.
      -- On a full rebuild, it expands to "1=1".
      -- ============================================================
      branch_app_trials AS (
      SELECT
      DATE(report_date)                AS report_date
      ,anonymous_id
      ,os                               AS device_os
      ,CASE
      WHEN LOWER(ad_partner) = 'facebook'                                   THEN 'meta'
      WHEN LOWER(ad_partner) IN ('google adwords','google ads','google')    THEN 'google'
      WHEN LOWER(ad_partner) LIKE '%tiktok%'                                THEN 'tiktok'
      WHEN LOWER(ad_partner) LIKE '%apple%search%'                          THEN 'apple_search'
      WHEN LOWER(ad_partner) LIKE '%snapchat%'                              THEN 'snapchat'
      WHEN LOWER(ad_partner) LIKE '%roku%'                                  THEN 'roku'
      WHEN LOWER(ad_partner) = 'unattributed'                               THEN 'unattributed'
      ELSE LOWER(ad_partner)
      END                              AS campaign_source
      ,channel                          AS branch_channel
      ,campaign                         AS campaign_name
      ,campaign_id
      ,feature                          AS branch_feature
      ,creative_name
      ,count                            AS app_trial_count
      ,CASE WHEN LOWER(feature) = 'paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch
      ,ROW_NUMBER() OVER (
      ORDER BY report_date, anonymous_id, campaign, os, creative_name
      )                                AS branch_row_num
      FROM php.branch_purchase
      WHERE report_date >= (SELECT start_date FROM params)
      AND report_date <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND count > 0
      AND (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ),

      -- ============================================================
      -- BRANCH.IO APP INSTALLS — from php.branch_install
      -- ============================================================
      branch_app_installs AS (
      SELECT
      DATE(report_date)                AS report_date
      ,anonymous_id
      ,os                               AS device_os
      ,CASE
      WHEN LOWER(ad_partner) = 'facebook'                                   THEN 'meta'
      WHEN LOWER(ad_partner) IN ('google adwords','google ads','google')    THEN 'google'
      WHEN LOWER(ad_partner) LIKE '%tiktok%'                                THEN 'tiktok'
      WHEN LOWER(ad_partner) LIKE '%apple%search%'                          THEN 'apple_search'
      WHEN LOWER(ad_partner) LIKE '%snapchat%'                              THEN 'snapchat'
      WHEN LOWER(ad_partner) LIKE '%roku%'                                  THEN 'roku'
      WHEN LOWER(ad_partner) = 'unattributed'                               THEN 'unattributed'
      ELSE LOWER(ad_partner)
      END                              AS campaign_source
      ,channel                          AS branch_channel
      ,campaign                         AS campaign_name
      ,campaign_id
      ,feature                          AS branch_feature
      ,creative_name
      ,count                            AS app_install_count
      ,CASE WHEN LOWER(feature) = 'paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch
      ,ROW_NUMBER() OVER (
      ORDER BY report_date, anonymous_id, campaign, os, creative_name
      )                                AS branch_row_num
      FROM php.branch_install
      WHERE report_date >= (SELECT start_date FROM params)
      AND report_date <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND count > 0
      AND (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ),

      -- ============================================================
      -- BRANCH.IO APP REINSTALLS — from php.branch_reinstall
      -- Mirrors branch_app_installs; tracks users who reinstalled the app.
      -- ============================================================
      branch_app_reinstalls AS (
      SELECT
      DATE(report_date)                AS report_date
      ,anonymous_id
      ,os                               AS device_os
      ,CASE
      WHEN LOWER(ad_partner) = 'facebook'                                   THEN 'meta'
      WHEN LOWER(ad_partner) IN ('google adwords','google ads','google')    THEN 'google'
      WHEN LOWER(ad_partner) LIKE '%tiktok%'                                THEN 'tiktok'
      WHEN LOWER(ad_partner) LIKE '%apple%search%'                          THEN 'apple_search'
      WHEN LOWER(ad_partner) LIKE '%snapchat%'                              THEN 'snapchat'
      WHEN LOWER(ad_partner) LIKE '%roku%'                                  THEN 'roku'
      WHEN LOWER(ad_partner) = 'unattributed'                               THEN 'unattributed'
      ELSE LOWER(ad_partner)
      END                              AS campaign_source
      ,channel                          AS branch_channel
      ,campaign                         AS campaign_name
      ,campaign_id
      ,feature                          AS branch_feature
      ,creative_name
      ,count                            AS app_reinstall_count
      ,CASE WHEN LOWER(feature) = 'paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch
      ,ROW_NUMBER() OVER (
      ORDER BY report_date, anonymous_id, campaign, os, creative_name
      )                                AS branch_row_num
      FROM php.branch_reinstall
      WHERE report_date >= (SELECT start_date FROM params)
      AND report_date <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND count > 0
      AND (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ),

      -- ============================================================
      -- LIFECYCLE EVENTS (web pipeline)
      -- Each UNION branch gets its own incrementcondition filter.
      -- ============================================================
      lifecycle_events AS (
      SELECT
      user_id, context_ip, anonymous_id
      ,received_at                AS event_received_at
      ,DATE(received_at)          AS event_date
      ,'page_visit'               AS source_event_type
      ,id                         AS event_id
      ,context_campaign_source    AS campaign_source
      ,context_campaign_name      AS campaign_name
      ,context_campaign_id        AS campaign_id
      ,context_campaign_medium    AS campaign_medium
      ,context_campaign_content   AS campaign_content
      ,CAST(NULL AS VARCHAR(255)) AS order_id
      ,CAST(NULL AS VARCHAR(255)) AS bundle_plan
      ,CAST(NULL AS VARCHAR(20))  AS trial_type
      FROM javascript_upff_home.pages
      WHERE received_at >= (SELECT start_date FROM params)
      AND received_at <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )

      UNION ALL

      SELECT
      user_id, context_ip, anonymous_id
      ,received_at, DATE(received_at)
      ,'free_trial', order_id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255))
      ,order_id, CAST(NULL AS VARCHAR(255)), CAST('standard' AS VARCHAR(20))
      FROM javaScript_upentertainment_checkout.order_completed
      WHERE received_at >= (SELECT start_date FROM params)
      AND received_at <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND user_id IS NOT NULL
      AND brand = 'upfaithandfamily'
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )

      UNION ALL

      SELECT
      user_id, context_ip, anonymous_id
      ,received_at, DATE(received_at)
      ,'free_trial', order_id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255))
      ,order_id, bundle_plan, CAST('bundle' AS VARCHAR(20))
      FROM javaScript_upentertainment_checkout.order_updated
      WHERE received_at >= (SELECT start_date FROM params)
      AND received_at <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND user_id IS NOT NULL
      AND brand = 'upfaithandfamily'
      AND bundle_plan IS NOT NULL
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )

      UNION ALL

      SELECT
      user_id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,received_at, DATE(received_at)
      ,'activation', id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(20))
      FROM chargebee_webhook_events.subscription_activated
      WHERE DATE(received_at) BETWEEN (SELECT start_date FROM params)
      AND (SELECT end_date   FROM params)
      AND content_subscription_subscription_items LIKE '%UP%'
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )

      UNION ALL

      SELECT
      user_id, context_ip, anonymous_id
      ,received_at, DATE(received_at)
      ,'reacquisition', id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(20))
      FROM javascript_upentertainment_checkout.order_resubscribed
      WHERE received_at >= (SELECT start_date FROM params)
      AND received_at <  (SELECT end_date   FROM params) + INTERVAL '1 day'
      AND user_id IS NOT NULL
      AND brand = 'upfaithandfamily'
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )

      UNION ALL

      SELECT
      user_id
      ,CAST(NULL AS VARCHAR(255))     AS context_ip
      ,CAST(NULL AS VARCHAR(255))     AS anonymous_id
      ,event_received_at, event_date
      ,'not_retained', event_id
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255))
      ,CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(255)), CAST(NULL AS VARCHAR(20))
      FROM (
      SELECT
      user_id
      ,received_at        AS event_received_at
      ,DATE(received_at)  AS event_date
      ,id                 AS event_id
      ,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY received_at ASC) AS rn
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE DATE(received_at) BETWEEN (SELECT start_date FROM params)
      AND (SELECT end_date   FROM params)
      AND content_subscription_subscription_items LIKE '%UP%'
      AND content_subscription_cancelled_at IS NOT NULL
      AND content_subscription_trial_end    IS NOT NULL
      AND (content_subscription_cancelled_at - content_subscription_trial_end
      < (SELECT cancel_window_seconds FROM params))
      AND user_id IS NOT NULL
      AND (
      {% incrementcondition %} received_at {% endincrementcondition %}
      )
      ) cancels_deduped
      WHERE rn = 1
      ),

      marketing_touches AS (
      SELECT
      context_ip, anonymous_id
      ,event_received_at  AS received_at
      ,event_date
      ,COALESCE(campaign_source,  'organic')  AS campaign_source
      ,COALESCE(campaign_name,    'organic')  AS campaign_name
      ,campaign_id
      ,COALESCE(campaign_medium,  'organic')  AS campaign_medium
      ,campaign_content
      ,event_id           AS touch_event_id
      ,user_id
      FROM lifecycle_events
      WHERE source_event_type = 'page_visit'
      ),

      user_lifecycle_flags AS (
      SELECT
      user_id
      ,MIN(CASE WHEN source_event_type = 'activation'    THEN event_date END) AS activation_date
      ,MIN(CASE WHEN source_event_type = 'reacquisition' THEN event_date END) AS reacquisition_date
      ,MIN(CASE WHEN source_event_type = 'not_retained'  THEN event_date END) AS not_retained_date
      ,CAST(MAX(CASE WHEN source_event_type = 'not_retained' THEN 1 ELSE 0 END) AS BOOLEAN) AS is_not_retained
      FROM lifecycle_events
      WHERE source_event_type IN ('activation', 'reacquisition', 'not_retained')
      AND user_id IS NOT NULL
      GROUP BY user_id
      ),

      free_trials AS (
      SELECT
      r.user_id, r.context_ip, r.anonymous_id, r.order_id
      ,r.event_received_at  AS conversion_event_at
      ,'free_trial'         AS conversion_event_type
      ,r.trial_type, r.bundle_plan
      ,CASE WHEN r.bundle_plan IS NOT NULL THEN TRUE ELSE FALSE END AS is_bundle_user
      ,ulf.activation_date, ulf.reacquisition_date
      ,COALESCE(ulf.is_not_retained, FALSE) AS is_not_retained
      ,ulf.not_retained_date
      ,CASE
      WHEN ulf.activation_date IS NOT NULL
      AND ulf.activation_date >= DATE(r.event_received_at) THEN 'activation'
      WHEN ulf.reacquisition_date IS NOT NULL                THEN 'reacquisition'
      ELSE NULL
      END AS lifecycle_event_type
      ,CASE
      WHEN ulf.activation_date IS NOT NULL
      AND ulf.activation_date >= DATE(r.event_received_at) THEN ulf.activation_date
      ELSE ulf.reacquisition_date
      END AS lifecycle_event_date
      ,CASE
      WHEN (ulf.activation_date IS NOT NULL
      AND ulf.activation_date >= DATE(r.event_received_at))
      OR ulf.reacquisition_date IS NOT NULL THEN TRUE
      ELSE FALSE
      END AS is_activated_or_reacquired
      ,COALESCE(uav.activation_value_dollars, 0)        AS activation_value
      ,COALESCE(uav.is_yearly_plan, FALSE)              AS is_yearly_plan
      ,COALESCE(uav.plan_type, 'unknown')               AS plan_type
      FROM (
      SELECT
      user_id, context_ip, anonymous_id, order_id, event_received_at,
      trial_type, bundle_plan
      ,ROW_NUMBER() OVER (
      PARTITION BY user_id, order_id
      ORDER BY CASE WHEN bundle_plan IS NOT NULL THEN 0 ELSE 1 END,
      event_received_at ASC
      ) AS dedup_rank
      FROM lifecycle_events
      WHERE source_event_type = 'free_trial'
      ) r
      LEFT JOIN user_lifecycle_flags ulf ON r.user_id = ulf.user_id
      LEFT JOIN user_activation_value uav ON r.user_id = uav.user_id
      WHERE r.dedup_rank = 1
      ),

      reacquisitions AS (
      SELECT
      le.user_id, le.context_ip, le.anonymous_id
      ,le.event_id              AS order_id
      ,le.event_received_at     AS conversion_event_at
      ,'reacquisition'          AS conversion_event_type
      ,CAST(NULL AS VARCHAR(20))  AS trial_type
      ,CAST(NULL AS VARCHAR(255)) AS bundle_plan
      ,FALSE                    AS is_bundle_user
      ,CAST(NULL AS DATE)       AS activation_date
      ,le.event_date            AS reacquisition_date
      ,FALSE                    AS is_not_retained
      ,CAST(NULL AS DATE)       AS not_retained_date
      ,'reacquisition'          AS lifecycle_event_type
      ,le.event_date            AS lifecycle_event_date
      ,TRUE                     AS is_activated_or_reacquired
      ,COALESCE(uav.activation_value_dollars, 0)        AS activation_value
      ,COALESCE(uav.is_yearly_plan, FALSE)              AS is_yearly_plan
      ,COALESCE(uav.plan_type, 'unknown')               AS plan_type
      FROM lifecycle_events le
      LEFT JOIN user_activation_value uav ON le.user_id = uav.user_id
      WHERE le.source_event_type = 'reacquisition'
      ),

      conversion_events AS (
      SELECT * FROM free_trials
      UNION ALL SELECT * FROM reacquisitions
      ),

      attributed_touches AS (
      SELECT
      c.order_id, c.user_id
      ,c.conversion_event_at, c.conversion_event_type
      ,c.trial_type, c.bundle_plan, c.is_bundle_user
      ,c.lifecycle_event_date, c.lifecycle_event_type
      ,c.is_activated_or_reacquired, c.is_not_retained
      ,c.activation_value, c.is_yearly_plan, c.plan_type
      ,mt.received_at        AS touch_received_at
      ,mt.campaign_source, mt.campaign_name, mt.campaign_id
      ,mt.campaign_medium, mt.campaign_content
      ,DATEDIFF(day, mt.received_at, c.conversion_event_at) AS days_before_conversion
      ,ROW_NUMBER() OVER (
      PARTITION BY c.conversion_event_type, c.order_id
      ORDER BY mt.received_at ASC
      ) AS first_touch_rank
      ,ROW_NUMBER() OVER (
      PARTITION BY c.conversion_event_type, c.order_id
      ORDER BY mt.received_at DESC
      ) AS last_touch_rank
      ,COUNT(*) OVER (PARTITION BY c.conversion_event_type, c.order_id) AS total_touches
      FROM conversion_events c
      JOIN marketing_touches mt
      ON (mt.anonymous_id = c.anonymous_id OR mt.context_ip = c.context_ip)
      AND mt.received_at <= c.conversion_event_at
      AND mt.received_at >= c.conversion_event_at
      - ((SELECT max_attribution_window_days FROM params) || ' days')::INTERVAL
      ),

      direct_conversions AS (
      SELECT
      c.order_id, c.user_id
      ,c.conversion_event_at, c.conversion_event_type
      ,c.trial_type, c.bundle_plan, c.is_bundle_user
      ,c.lifecycle_event_date, c.lifecycle_event_type
      ,c.is_activated_or_reacquired, c.is_not_retained
      ,c.activation_value, c.is_yearly_plan, c.plan_type
      ,0                              AS total_touches
      ,CAST(NULL AS TIMESTAMP)        AS attributed_touch_at
      ,CAST('direct' AS VARCHAR(20))  AS attributed_campaign_source
      ,CAST('direct' AS VARCHAR(20))  AS attributed_campaign_name
      ,CAST(NULL AS VARCHAR(255))     AS attributed_campaign_id
      ,CAST('direct' AS VARCHAR(20))  AS attributed_campaign_medium
      ,CAST(NULL AS VARCHAR(255))     AS attributed_campaign_content
      FROM conversion_events c
      LEFT JOIN attributed_touches at
      ON c.order_id = at.order_id
      AND c.conversion_event_type = at.conversion_event_type
      WHERE at.order_id IS NULL
      ),

      first_last_match AS (
      SELECT
      f.order_id, f.conversion_event_type
      ,CASE
      WHEN COALESCE(f.campaign_source, '') = COALESCE(l.campaign_source, '')
      AND COALESCE(f.campaign_medium, '') = COALESCE(l.campaign_medium, '')
      AND COALESCE(f.campaign_name,   '') = COALESCE(l.campaign_name,   '')
      THEN TRUE ELSE FALSE
      END AS is_first_and_last
      FROM attributed_touches f
      JOIN attributed_touches l
      ON f.order_id = l.order_id
      AND f.conversion_event_type = l.conversion_event_type
      WHERE f.first_touch_rank = 1 AND l.last_touch_rank  = 1
      ),

      all_touches_raw AS (
      SELECT
      at.order_id, at.user_id
      ,at.conversion_event_at, at.conversion_event_type
      ,at.trial_type, at.bundle_plan, at.is_bundle_user
      ,at.lifecycle_event_date, at.lifecycle_event_type
      ,at.is_activated_or_reacquired, at.is_not_retained
      ,at.activation_value, at.is_yearly_plan, at.plan_type
      ,at.total_touches
      ,at.touch_received_at  AS attributed_touch_at
      ,at.days_before_conversion
      ,CAST(at.campaign_source  AS VARCHAR(255)) AS attributed_campaign_source
      ,CAST(at.campaign_name    AS VARCHAR(255)) AS attributed_campaign_name
      ,CAST(at.campaign_id      AS VARCHAR(255)) AS attributed_campaign_id
      ,CAST(at.campaign_medium  AS VARCHAR(255)) AS attributed_campaign_medium
      ,CAST(at.campaign_content AS VARCHAR(255)) AS attributed_campaign_content
      ,at.first_touch_rank, at.last_touch_rank
      ,CAST(CASE WHEN at.first_touch_rank = 1 THEN 1.0 ELSE 0.0 END
      AS NUMERIC(7,6)) AS credit_first_touch
      ,CAST(CASE WHEN at.last_touch_rank = 1 THEN 1.0 ELSE 0.0 END
      AS NUMERIC(7,6)) AS credit_last_touch
      ,CAST(CASE
      WHEN flm.is_first_and_last
      AND at.first_touch_rank = 1            THEN 1.0
      WHEN flm.is_first_and_last                  THEN 0.0
      WHEN at.first_touch_rank = 1                THEN 0.5
      WHEN at.last_touch_rank  = 1                THEN 0.5
      ELSE                                              0.0
      END AS NUMERIC(7,6)) AS credit_first_last
      ,CAST(CASE
      WHEN at.total_touches = 1                                  THEN 1.0
      WHEN at.total_touches = 2 AND at.first_touch_rank = 1      THEN 0.5
      WHEN at.total_touches = 2 AND at.last_touch_rank  = 1      THEN 0.5
      WHEN at.first_touch_rank = 1                               THEN 0.4
      WHEN at.last_touch_rank  = 1                               THEN 0.4
      ELSE COALESCE(0.2 / NULLIF(at.total_touches - 2, 0), 0)
      END AS NUMERIC(7,6)) AS credit_position_based
      ,CAST(CASE
      WHEN at.total_touches = 1                       THEN 'only'
      WHEN at.first_touch_rank = 1                    THEN 'first'
      WHEN at.last_touch_rank  = 1                    THEN 'last'
      ELSE                                                 'middle'
      END AS VARCHAR(20)) AS touch_position
      ,COALESCE(flm.is_first_and_last, FALSE) AS is_first_and_last
      FROM attributed_touches at
      LEFT JOIN first_last_match flm
      ON at.order_id              = flm.order_id
      AND at.conversion_event_type = flm.conversion_event_type
      ),

      selected_attributed_touches AS (
      SELECT
      order_id, user_id
      ,conversion_event_at, conversion_event_type
      ,trial_type, bundle_plan, is_bundle_user
      ,lifecycle_event_date, lifecycle_event_type
      ,is_activated_or_reacquired, is_not_retained
      ,activation_value, is_yearly_plan, plan_type
      ,total_touches
      ,MIN(days_before_conversion)         AS min_days_before_conversion
      ,MIN(attributed_touch_at)            AS attributed_touch_at
      ,attributed_campaign_source
      ,attributed_campaign_name
      ,MAX(attributed_campaign_id)         AS attributed_campaign_id
      ,attributed_campaign_medium
      ,MAX(attributed_campaign_content)    AS attributed_campaign_content
      ,CAST(SUM(credit_first_touch)    AS NUMERIC(7,6)) AS credit_first_touch
      ,CAST(SUM(credit_last_touch)     AS NUMERIC(7,6)) AS credit_last_touch
      ,CAST(SUM(credit_first_last)     AS NUMERIC(7,6)) AS credit_first_last
      ,CAST(SUM(credit_position_based) AS NUMERIC(7,6)) AS credit_position_based
      ,MAX(touch_position)                 AS touch_position
      ,BOOL_OR(first_touch_rank = 1)       AS is_first_touch
      ,BOOL_OR(last_touch_rank = 1)        AS is_last_touch
      ,BOOL_OR(is_first_and_last)          AS is_first_and_last
      FROM all_touches_raw
      GROUP BY
      order_id, user_id, conversion_event_at, conversion_event_type
      ,trial_type, bundle_plan, is_bundle_user
      ,lifecycle_event_date, lifecycle_event_type, is_activated_or_reacquired
      ,is_not_retained, activation_value, is_yearly_plan, plan_type
      ,total_touches
      ,attributed_campaign_source
      ,attributed_campaign_name
      ,attributed_campaign_medium
      ),

      selected_attributed_direct AS (
      SELECT
      order_id, user_id, conversion_event_at, conversion_event_type
      ,trial_type, bundle_plan, is_bundle_user
      ,lifecycle_event_date, lifecycle_event_type, is_activated_or_reacquired
      ,is_not_retained
      ,activation_value, is_yearly_plan, plan_type
      ,total_touches
      ,CAST(NULL AS INTEGER)        AS min_days_before_conversion
      ,attributed_touch_at
      ,CAST(attributed_campaign_source  AS VARCHAR(255)) AS attributed_campaign_source
      ,CAST(attributed_campaign_name    AS VARCHAR(255)) AS attributed_campaign_name
      ,CAST(attributed_campaign_id      AS VARCHAR(255)) AS attributed_campaign_id
      ,CAST(attributed_campaign_medium  AS VARCHAR(255)) AS attributed_campaign_medium
      ,CAST(attributed_campaign_content AS VARCHAR(255)) AS attributed_campaign_content
      ,CAST(1.0 AS NUMERIC(7,6))    AS credit_first_touch
      ,CAST(1.0 AS NUMERIC(7,6))    AS credit_last_touch
      ,CAST(1.0 AS NUMERIC(7,6))    AS credit_first_last
      ,CAST(1.0 AS NUMERIC(7,6))    AS credit_position_based
      ,CAST('only' AS VARCHAR(20))  AS touch_position
      ,TRUE                         AS is_first_touch
      ,TRUE                         AS is_last_touch
      ,TRUE                         AS is_first_and_last
      FROM direct_conversions
      ),

      selected_attributed AS (
      SELECT * FROM selected_attributed_touches
      UNION ALL SELECT * FROM selected_attributed_direct
      ),

      daily_campaign_metrics AS (
      SELECT
      DATE(sa.conversion_event_at)   AS report_date
      ,sa.attributed_campaign_name    AS campaign_name
      ,sa.attributed_campaign_medium  AS campaign_medium
      ,sa.attributed_campaign_content AS campaign_content
      ,SUM(CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.trial_type = 'standard'
      AND sa.is_last_touch
      THEN 1 ELSE 0 END)        AS standard_trials
      ,SUM(CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.trial_type = 'bundle'
      AND sa.is_last_touch
      THEN 1 ELSE 0 END)        AS bundle_trials
      ,SUM(CASE WHEN sa.conversion_event_type = 'reacquisition'
      AND sa.is_last_touch
      THEN 1 ELSE 0 END)        AS reacquisitions
      ,COUNT(DISTINCT CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.lifecycle_event_type = 'activation'
      AND sa.is_last_touch
      THEN sa.user_id END) AS activations
      ,COUNT(DISTINCT CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.is_last_touch
      THEN sa.user_id END) AS unique_trial_users
      ,COUNT(DISTINCT CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.is_not_retained
      AND sa.is_last_touch
      THEN sa.user_id END)
      * 1.0
      / NULLIF(COUNT(DISTINCT CASE WHEN sa.conversion_event_type = 'free_trial'
      AND sa.is_last_touch
      THEN sa.user_id END), 0)
      AS churn_rate_raw
      ,SUM(CASE WHEN sa.is_last_touch AND sa.activation_value > 0
      THEN sa.activation_value ELSE 0 END)              AS total_order_value
      ,AVG(CASE WHEN sa.is_last_touch AND sa.activation_value > 0
      THEN sa.activation_value END)                     AS avg_order_value
      ,COUNT(DISTINCT CASE WHEN sa.is_last_touch AND sa.is_yearly_plan
      THEN sa.user_id END)                   AS yearly_plan_users
      ,COUNT(DISTINCT CASE WHEN sa.is_last_touch
      AND sa.activation_value > 0
      AND NOT sa.is_yearly_plan
      THEN sa.user_id END)                   AS monthly_plan_users
      FROM selected_attributed sa
      GROUP BY 1, 2, 3, 4
      ),

      daily_norms AS (
      SELECT
      report_date
      ,GREATEST(MAX(standard_trials), 1)  AS max_standard
      ,GREATEST(MAX(bundle_trials), 1)    AS max_bundle
      ,GREATEST(MAX(reacquisitions), 1)   AS max_reacq
      ,GREATEST(MAX(activations), 1)      AS max_activations
      FROM daily_campaign_metrics
      GROUP BY report_date
      ),

      daily_campaign_quality AS (
      SELECT
      m.report_date, m.campaign_name, m.campaign_medium, m.campaign_content
      ,m.standard_trials, m.bundle_trials, m.reacquisitions, m.activations
      ,m.unique_trial_users
      ,CAST(COALESCE(m.total_order_value, 0)  AS NUMERIC(12,2)) AS total_order_value
      ,CAST(COALESCE(m.avg_order_value, 0)    AS NUMERIC(10,2)) AS avg_order_value
      ,COALESCE(m.yearly_plan_users, 0)       AS yearly_plan_users
      ,COALESCE(m.monthly_plan_users, 0)      AS monthly_plan_users
      ,CAST(
      CASE WHEN m.unique_trial_users > 0
      THEN 1.0 * m.activations / m.unique_trial_users
      ELSE 0 END
      AS NUMERIC(5,4)
      ) AS trial_to_paid_rate
      ,CAST(COALESCE(m.churn_rate_raw, 0)         AS NUMERIC(5,4)) AS churn_rate
      ,CAST(1.0 - COALESCE(m.churn_rate_raw, 0)   AS NUMERIC(5,4)) AS retention_rate
      ,CAST(100.0 * m.standard_trials  / n.max_standard    AS NUMERIC(10,4)) AS std_trial_score
      ,CAST(100.0 * m.bundle_trials    / n.max_bundle      AS NUMERIC(10,4)) AS bundle_trial_score
      ,CAST(100.0 * m.reacquisitions   / n.max_reacq       AS NUMERIC(10,4)) AS reacq_score
      ,CAST(100.0 * m.activations      / n.max_activations AS NUMERIC(10,4)) AS activation_score
      ,CAST(
      (SELECT w_standard_trials FROM params) * (100.0 * m.standard_trials  / n.max_standard)
      + (SELECT w_bundle_trials   FROM params) * (100.0 * m.bundle_trials    / n.max_bundle)
      + (SELECT w_reacquisitions  FROM params) * (100.0 * m.reacquisitions   / n.max_reacq)
      + (SELECT w_activations     FROM params) * (100.0 * m.activations      / n.max_activations)
      AS NUMERIC(10,4)
      ) AS volume_score
      ,CAST(
      CAST(
      (SELECT w_standard_trials FROM params) * (100.0 * m.standard_trials  / n.max_standard)
      + (SELECT w_bundle_trials   FROM params) * (100.0 * m.bundle_trials    / n.max_bundle)
      + (SELECT w_reacquisitions  FROM params) * (100.0 * m.reacquisitions   / n.max_reacq)
      + (SELECT w_activations     FROM params) * (100.0 * m.activations      / n.max_activations)
      AS NUMERIC(10,4)
      )
      * CAST(
      0.5 + LEAST(
      (CASE WHEN m.unique_trial_users > 0
      THEN 1.0 * m.activations / m.unique_trial_users
      ELSE 0 END)
      / (SELECT conv_rate_max FROM params), 1.0
      ) AS NUMERIC(5,4)
      )
      * CAST(
      GREATEST(
      1.0 - COALESCE(m.churn_rate_raw, 0),
      1.0 - (SELECT retention_floor FROM params)
      ) AS NUMERIC(5,4)
      )
      AS NUMERIC(10,4)
      ) AS quality_score
      FROM daily_campaign_metrics m
      JOIN daily_norms n ON m.report_date = n.report_date
      ),

      page_visit_rows AS (
      SELECT
      CAST('page_visit' AS VARCHAR(20))      AS event_type
      ,touch_event_id                         AS event_id
      ,user_id, context_ip, anonymous_id
      ,received_at                            AS event_at
      ,event_date                             AS report_date
      ,CAST(campaign_source AS VARCHAR(255))  AS campaign_source
      ,CAST(campaign_name   AS VARCHAR(255))  AS campaign_name
      ,campaign_id
      ,CAST(campaign_medium AS VARCHAR(255))  AS campaign_medium
      ,campaign_content
      ,CAST(NULL AS VARCHAR(255)) AS order_id
      ,CAST(NULL AS VARCHAR(20))  AS conversion_event_type
      ,CAST(NULL AS VARCHAR(20))  AS trial_type
      ,CAST(NULL AS VARCHAR(255)) AS bundle_plan
      ,FALSE                      AS is_bundle_user
      ,CAST(NULL AS VARCHAR(20))  AS lifecycle_event_type
      ,CAST(NULL AS DATE)         AS lifecycle_event_date
      ,FALSE                      AS is_activated_or_reacquired
      ,FALSE                      AS is_not_retained
      ,CAST(0.0 AS NUMERIC(10,2)) AS activation_value
      ,FALSE                      AS is_yearly_plan
      ,CAST(NULL AS VARCHAR(20))  AS plan_type
      ,0                          AS total_touches
      ,CAST(NULL AS INTEGER)      AS min_days_before_conversion
      ,CAST(NULL AS TIMESTAMP)    AS attributed_touch_at
      ,CAST(0.0 AS NUMERIC(7,6))  AS credit_first_touch
      ,CAST(0.0 AS NUMERIC(7,6))  AS credit_last_touch
      ,CAST(0.0 AS NUMERIC(7,6))  AS credit_first_last
      ,CAST(0.0 AS NUMERIC(7,6))  AS credit_position_based
      ,CAST(NULL AS VARCHAR(20))  AS touch_position
      ,FALSE                      AS is_first_touch
      ,FALSE                      AS is_last_touch
      ,FALSE                      AS is_first_and_last
      ,CAST(0 AS INTEGER)         AS app_trial_count
      ,CAST(0 AS INTEGER)         AS app_install_count
      ,CAST(0 AS INTEGER)         AS app_reinstall_count
      ,CAST(NULL AS VARCHAR(50))  AS device_os
      ,CAST(NULL AS VARCHAR(255)) AS branch_channel
      ,CAST(NULL AS VARCHAR(100)) AS branch_feature
      ,CAST(NULL AS VARCHAR(500)) AS creative_name
      ,FALSE                      AS is_paid_branch
      ,CAST(NULL AS NUMERIC(10,2))  AS quality_score
      ,CAST(NULL AS NUMERIC(10,2))  AS volume_score
      ,CAST(NULL AS NUMERIC(5,4))   AS trial_to_paid_rate
      ,CAST(NULL AS NUMERIC(5,4))   AS churn_rate
      ,CAST(NULL AS NUMERIC(5,4))   AS retention_rate
      ,CAST(NULL AS NUMERIC(12,2))  AS campaign_total_aov
      ,CAST(NULL AS NUMERIC(10,2))  AS campaign_avg_aov
      ,CAST(NULL AS NUMERIC(10,2))  AS std_trial_score
      ,CAST(NULL AS NUMERIC(10,2))  AS bundle_trial_score
      ,CAST(NULL AS NUMERIC(10,2))  AS reacq_score
      ,CAST(NULL AS NUMERIC(10,2))  AS activation_score
      FROM marketing_touches
      ),

      conversion_rows AS (
      SELECT
      CAST('conversion' AS VARCHAR(20))           AS event_type
      ,sa.order_id                                 AS event_id
      ,sa.user_id
      ,CAST(NULL AS VARCHAR(255))                  AS context_ip
      ,CAST(NULL AS VARCHAR(255))                  AS anonymous_id
      ,sa.conversion_event_at                      AS event_at
      ,DATE(sa.conversion_event_at)                AS report_date
      ,sa.attributed_campaign_source               AS campaign_source
      ,sa.attributed_campaign_name                 AS campaign_name
      ,sa.attributed_campaign_id                   AS campaign_id
      ,sa.attributed_campaign_medium               AS campaign_medium
      ,sa.attributed_campaign_content              AS campaign_content
      ,sa.order_id, sa.conversion_event_type
      ,sa.trial_type, sa.bundle_plan, sa.is_bundle_user
      ,sa.lifecycle_event_type, sa.lifecycle_event_date
      ,sa.is_activated_or_reacquired, sa.is_not_retained
      ,CAST(sa.activation_value    AS NUMERIC(10,2)) AS activation_value
      ,sa.is_yearly_plan, sa.plan_type
      ,sa.total_touches
      ,sa.min_days_before_conversion
      ,sa.attributed_touch_at
      ,sa.credit_first_touch, sa.credit_last_touch
      ,sa.credit_first_last, sa.credit_position_based
      ,sa.touch_position
      ,sa.is_first_touch, sa.is_last_touch, sa.is_first_and_last
      ,CAST(0 AS INTEGER)         AS app_trial_count
      ,CAST(0 AS INTEGER)         AS app_install_count
      ,CAST(0 AS INTEGER)         AS app_reinstall_count
      ,CAST(NULL AS VARCHAR(50))  AS device_os
      ,CAST(NULL AS VARCHAR(255)) AS branch_channel
      ,CAST(NULL AS VARCHAR(100)) AS branch_feature
      ,CAST(NULL AS VARCHAR(500)) AS creative_name
      ,FALSE                      AS is_paid_branch
      ,CAST(dq.quality_score      AS NUMERIC(10,2)) AS quality_score
      ,CAST(dq.volume_score       AS NUMERIC(10,2)) AS volume_score
      ,CAST(dq.trial_to_paid_rate AS NUMERIC(5,4))  AS trial_to_paid_rate
      ,CAST(dq.churn_rate         AS NUMERIC(5,4))  AS churn_rate
      ,CAST(dq.retention_rate     AS NUMERIC(5,4))  AS retention_rate
      ,CAST(dq.total_order_value  AS NUMERIC(12,2)) AS campaign_total_aov
      ,CAST(dq.avg_order_value    AS NUMERIC(10,2)) AS campaign_avg_aov
      ,CAST(dq.std_trial_score    AS NUMERIC(10,2)) AS std_trial_score
      ,CAST(dq.bundle_trial_score AS NUMERIC(10,2)) AS bundle_trial_score
      ,CAST(dq.reacq_score        AS NUMERIC(10,2)) AS reacq_score
      ,CAST(dq.activation_score   AS NUMERIC(10,2)) AS activation_score
      FROM selected_attributed sa
      LEFT JOIN daily_campaign_quality dq
      ON DATE(sa.conversion_event_at) = dq.report_date
      AND COALESCE(sa.attributed_campaign_name, '')    = COALESCE(dq.campaign_name, '')
      AND COALESCE(sa.attributed_campaign_medium, '')  = COALESCE(dq.campaign_medium, '')
      AND COALESCE(sa.attributed_campaign_content, '') = COALESCE(dq.campaign_content, '')
      ),

      app_trial_rows AS (
      SELECT
      CAST('app_trial' AS VARCHAR(20))                                     AS event_type
      ,CAST(COALESCE(anonymous_id, 'no_aid') || '|' || branch_row_num::TEXT
      AS VARCHAR(255))                                                AS event_id
      ,CAST(NULL AS VARCHAR(255))                                           AS user_id
      ,CAST(NULL AS VARCHAR(255))                                           AS context_ip
      ,CAST(NULL AS VARCHAR(255))                                           AS anonymous_id
      ,report_date::TIMESTAMP                                               AS event_at
      ,report_date
      ,CAST(campaign_source  AS VARCHAR(255))                               AS campaign_source
      ,CAST(campaign_name    AS VARCHAR(255))                               AS campaign_name
      ,CAST(campaign_id      AS VARCHAR(255))                               AS campaign_id
      ,CAST('paid' AS VARCHAR(255))                                         AS campaign_medium
      ,CAST(NULL AS VARCHAR(255))                                           AS campaign_content
      ,CAST(NULL AS VARCHAR(255))                                           AS order_id
      ,'free_trial'                                                         AS conversion_event_type
      ,CAST('app' AS VARCHAR(20))                                           AS trial_type
      ,CAST(NULL AS VARCHAR(255))                                           AS bundle_plan
      ,FALSE                                                                AS is_bundle_user
      ,CAST(NULL AS VARCHAR(20))                                            AS lifecycle_event_type
      ,CAST(NULL AS DATE)                                                   AS lifecycle_event_date
      ,FALSE                                                                AS is_activated_or_reacquired
      ,FALSE                                                                AS is_not_retained
      ,CAST(0 AS NUMERIC(10,2))                                             AS activation_value
      ,FALSE                                                                AS is_yearly_plan
      ,CAST(NULL AS VARCHAR(20))                                            AS plan_type
      ,0                                                                    AS total_touches
      ,CAST(NULL AS INTEGER)                                                AS min_days_before_conversion
      ,CAST(NULL AS TIMESTAMP)                                              AS attributed_touch_at
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_last_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_last
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_position_based
      ,CAST(NULL AS VARCHAR(20))                                            AS touch_position
      ,FALSE                                                                AS is_first_touch
      ,FALSE                                                                AS is_last_touch
      ,FALSE                                                                AS is_first_and_last
      ,app_trial_count
      ,0                                                                    AS app_install_count
      ,0                                                                    AS app_reinstall_count
      ,device_os
      ,CAST(branch_channel AS VARCHAR(255))                                 AS branch_channel
      ,CAST(branch_feature AS VARCHAR(100))                                 AS branch_feature
      ,CAST(creative_name AS VARCHAR(500))                                  AS creative_name
      ,is_paid_branch
      ,CAST(NULL AS NUMERIC(10,2))                                          AS quality_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS volume_score
      ,CAST(NULL AS NUMERIC(5,4))                                           AS trial_to_paid_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS churn_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS retention_rate
      ,CAST(NULL AS NUMERIC(12,2))                                          AS campaign_total_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS campaign_avg_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS std_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS bundle_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS reacq_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS activation_score
      FROM branch_app_trials
      ),

      app_install_rows AS (
      SELECT
      CAST('app_install' AS VARCHAR(20))                                   AS event_type
      ,CAST(COALESCE(anonymous_id, 'no_aid') || '|' || branch_row_num::TEXT
      AS VARCHAR(255))                                                AS event_id
      ,CAST(NULL AS VARCHAR(255))                                           AS user_id
      ,CAST(NULL AS VARCHAR(255))                                           AS context_ip
      ,CAST(NULL AS VARCHAR(255))                                           AS anonymous_id
      ,report_date::TIMESTAMP                                               AS event_at
      ,report_date
      ,CAST(campaign_source  AS VARCHAR(255))                               AS campaign_source
      ,CAST(campaign_name    AS VARCHAR(255))                               AS campaign_name
      ,CAST(campaign_id      AS VARCHAR(255))                               AS campaign_id
      ,CAST('paid' AS VARCHAR(255))                                         AS campaign_medium
      ,CAST(NULL AS VARCHAR(255))                                           AS campaign_content
      ,CAST(NULL AS VARCHAR(255))                                           AS order_id
      ,CAST(NULL AS VARCHAR(20))                                            AS conversion_event_type
      ,CAST('app_install' AS VARCHAR(20))                                   AS trial_type
      ,CAST(NULL AS VARCHAR(255))                                           AS bundle_plan
      ,FALSE                                                                AS is_bundle_user
      ,CAST(NULL AS VARCHAR(20))                                            AS lifecycle_event_type
      ,CAST(NULL AS DATE)                                                   AS lifecycle_event_date
      ,FALSE                                                                AS is_activated_or_reacquired
      ,FALSE                                                                AS is_not_retained
      ,CAST(0 AS NUMERIC(10,2))                                             AS activation_value
      ,FALSE                                                                AS is_yearly_plan
      ,CAST(NULL AS VARCHAR(20))                                            AS plan_type
      ,0                                                                    AS total_touches
      ,CAST(NULL AS INTEGER)                                                AS min_days_before_conversion
      ,CAST(NULL AS TIMESTAMP)                                              AS attributed_touch_at
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_last_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_last
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_position_based
      ,CAST(NULL AS VARCHAR(20))                                            AS touch_position
      ,FALSE                                                                AS is_first_touch
      ,FALSE                                                                AS is_last_touch
      ,FALSE                                                                AS is_first_and_last
      ,0                                                                    AS app_trial_count
      ,app_install_count
      ,0                                                                    AS app_reinstall_count
      ,device_os
      ,CAST(branch_channel AS VARCHAR(255))                                 AS branch_channel
      ,CAST(branch_feature AS VARCHAR(100))                                 AS branch_feature
      ,CAST(creative_name AS VARCHAR(500))                                  AS creative_name
      ,is_paid_branch
      ,CAST(NULL AS NUMERIC(10,2))                                          AS quality_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS volume_score
      ,CAST(NULL AS NUMERIC(5,4))                                           AS trial_to_paid_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS churn_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS retention_rate
      ,CAST(NULL AS NUMERIC(12,2))                                          AS campaign_total_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS campaign_avg_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS std_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS bundle_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS reacq_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS activation_score
      FROM branch_app_installs
      ),

      app_reinstall_rows AS (
      SELECT
      CAST('app_reinstall' AS VARCHAR(20))                                 AS event_type
      ,CAST(COALESCE(anonymous_id, 'no_aid') || '|' || branch_row_num::TEXT
      AS VARCHAR(255))                                                AS event_id
      ,CAST(NULL AS VARCHAR(255))                                           AS user_id
      ,CAST(NULL AS VARCHAR(255))                                           AS context_ip
      ,CAST(NULL AS VARCHAR(255))                                           AS anonymous_id
      ,report_date::TIMESTAMP                                               AS event_at
      ,report_date
      ,CAST(campaign_source  AS VARCHAR(255))                               AS campaign_source
      ,CAST(campaign_name    AS VARCHAR(255))                               AS campaign_name
      ,CAST(campaign_id      AS VARCHAR(255))                               AS campaign_id
      ,CAST('paid' AS VARCHAR(255))                                         AS campaign_medium
      ,CAST(NULL AS VARCHAR(255))                                           AS campaign_content
      ,CAST(NULL AS VARCHAR(255))                                           AS order_id
      ,CAST(NULL AS VARCHAR(20))                                            AS conversion_event_type
      ,CAST('app_reinstall' AS VARCHAR(20))                                 AS trial_type
      ,CAST(NULL AS VARCHAR(255))                                           AS bundle_plan
      ,FALSE                                                                AS is_bundle_user
      ,CAST(NULL AS VARCHAR(20))                                            AS lifecycle_event_type
      ,CAST(NULL AS DATE)                                                   AS lifecycle_event_date
      ,FALSE                                                                AS is_activated_or_reacquired
      ,FALSE                                                                AS is_not_retained
      ,CAST(0 AS NUMERIC(10,2))                                             AS activation_value
      ,FALSE                                                                AS is_yearly_plan
      ,CAST(NULL AS VARCHAR(20))                                            AS plan_type
      ,0                                                                    AS total_touches
      ,CAST(NULL AS INTEGER)                                                AS min_days_before_conversion
      ,CAST(NULL AS TIMESTAMP)                                              AS attributed_touch_at
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_last_touch
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_first_last
      ,CAST(0.0 AS NUMERIC(7,6))                                            AS credit_position_based
      ,CAST(NULL AS VARCHAR(20))                                            AS touch_position
      ,FALSE                                                                AS is_first_touch
      ,FALSE                                                                AS is_last_touch
      ,FALSE                                                                AS is_first_and_last
      ,0                                                                    AS app_trial_count
      ,0                                                                    AS app_install_count
      ,app_reinstall_count
      ,device_os
      ,CAST(branch_channel AS VARCHAR(255))                                 AS branch_channel
      ,CAST(branch_feature AS VARCHAR(100))                                 AS branch_feature
      ,CAST(creative_name AS VARCHAR(500))                                  AS creative_name
      ,is_paid_branch
      ,CAST(NULL AS NUMERIC(10,2))                                          AS quality_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS volume_score
      ,CAST(NULL AS NUMERIC(5,4))                                           AS trial_to_paid_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS churn_rate
      ,CAST(NULL AS NUMERIC(5,4))                                           AS retention_rate
      ,CAST(NULL AS NUMERIC(12,2))                                          AS campaign_total_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS campaign_avg_aov
      ,CAST(NULL AS NUMERIC(10,2))                                          AS std_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS bundle_trial_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS reacq_score
      ,CAST(NULL AS NUMERIC(10,2))                                          AS activation_score
      FROM branch_app_reinstalls
      )

      SELECT * FROM page_visit_rows
      UNION ALL
      SELECT * FROM conversion_rows
      UNION ALL
      SELECT * FROM app_trial_rows
      UNION ALL
      SELECT * FROM app_install_rows
      UNION ALL
      SELECT * FROM app_reinstall_rows
      ;;
  }

  ##############################################################
  # PARAMETERS
  ##############################################################
  parameter: attribution_model {
    type: unquoted
    label: "Attribution Model"
    default_value: "last_touch"
    allowed_value: { label: "First Touch"               value: "first_touch" }
    allowed_value: { label: "Last Touch"                value: "last_touch" }
    allowed_value: { label: "First + Last Touch"        value: "first_last" }
    allowed_value: { label: "Position-Based (U-Shaped)" value: "position_based" }
  }

  parameter: attribution_window_days {
    type: unquoted
    label: "Attribution Window"
    default_value: "30"
    allowed_value: { label: "1 day"   value: "1" }
    allowed_value: { label: "3 days"  value: "3" }
    allowed_value: { label: "7 days"  value: "7" }
    allowed_value: { label: "14 days" value: "14" }
    allowed_value: { label: "30 days" value: "30" }
    allowed_value: { label: "60 days" value: "60" }
    allowed_value: { label: "90 days" value: "90" }
  }

  ##############################################################
  # IDENTITY DIMENSIONS
  ##############################################################
  dimension: event_id {
    type: string
    primary_key: yes
    sql:
      ${TABLE}.event_id || '|' ||
      ${TABLE}.event_type || '|' ||
      COALESCE(${TABLE}.touch_position, 'na') || '|' ||
      COALESCE(${TABLE}.campaign_name, 'no_campaign') || '|' ||
      COALESCE(${TABLE}.report_date::TEXT, 'no_date') || '|' ||
      COALESCE(${TABLE}.device_os, 'no_os') || '|' ||
      COALESCE(${TABLE}.creative_name, 'no_creative')
    ;;
  }

  dimension: event_type {
    type: string
    description: "'page_visit' | 'conversion' | 'app_trial' | 'app_install' | 'app_reinstall'"
    sql: ${TABLE}.event_type ;;
  }

  dimension: user_id        { type: string  sql: ${TABLE}.user_id ;; }
  dimension: anonymous_id   { type: string  sql: ${TABLE}.anonymous_id ;; }
  dimension: context_ip     { type: string  sql: ${TABLE}.context_ip ;; }

  ##############################################################
  # TIME DIMENSIONS
  ##############################################################
  dimension_group: event {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.event_at ;;
  }

  dimension_group: report_date {
    type: time
    label: "Report Date"
    timeframes: [date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: attributed_touch {
    type: time
    timeframes: [raw, time, date]
    sql: ${TABLE}.attributed_touch_at ;;
  }

  dimension: lifecycle_event_date {
    type: date
    convert_tz: no
    datatype: date
    sql: ${TABLE}.lifecycle_event_date ;;
  }

  ##############################################################
  # CAMPAIGN DIMENSIONS
  ##############################################################
  dimension: campaign_source   { type: string  label: "Campaign Source"   sql: ${TABLE}.campaign_source ;; }
  dimension: campaign_name     { type: string  label: "Campaign Name"     sql: ${TABLE}.campaign_name ;; }
  dimension: campaign_id       { type: string  label: "Campaign ID"       sql: ${TABLE}.campaign_id ;; }
  dimension: campaign_medium   { type: string  label: "Campaign Medium"   sql: ${TABLE}.campaign_medium ;; }
  dimension: campaign_content  { type: string  label: "Campaign Content"  sql: ${TABLE}.campaign_content ;; }

  dimension: marketing_platform {
    type: string
    label: "Marketing Platform"
    description: "Normalized platform bucket"
    sql:
      CASE
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords')
             AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g')
             AND LOWER(${TABLE}.campaign_name) LIKE '%display%'                 THEN 'Google Display'
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords')
             AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g')
             AND (LOWER(${TABLE}.campaign_name) LIKE '%pmax%'
                  OR LOWER(${TABLE}.campaign_name) LIKE '%performance max%')    THEN 'Google PMax'
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords')
             AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g')    THEN 'Google Search'
        WHEN LOWER(${TABLE}.campaign_source) IN ('meta','instagram','ig','fb', 'an', 'campaign.name')
             OR LOWER(${TABLE}.campaign_source) LIKE 'meta%'                    THEN 'Meta Ads'
        WHEN LOWER(${TABLE}.campaign_source) IN ('bing','microsoft','msn')      THEN 'Bing Ads'
        WHEN LOWER(${TABLE}.campaign_source) IN ('hubspot', 'hubspot_upff', 'hubspot_uptv')
             OR LOWER(${TABLE}.campaign_medium) LIKE 'email%'                   THEN 'HubSpot'
        WHEN LOWER(${TABLE}.campaign_source) LIKE '%uptv%'                      THEN 'UPtv Digital'
        WHEN LOWER(${TABLE}.campaign_medium) = 'organic'
             AND LOWER(${TABLE}.campaign_source) IN ('google','bing','duckduckgo','yahoo') THEN 'Organic Search'
        WHEN LOWER(${TABLE}.campaign_medium) IN ('social','organic_social')
             OR (LOWER(${TABLE}.campaign_medium) = 'organic'
                 AND LOWER(${TABLE}.campaign_source) IN ('facebook','instagram','tiktok','x','twitter','linkedin'))
                                                                                THEN 'Organic Social'
        WHEN ${TABLE}.campaign_source = 'organic'                               THEN 'Others'
        WHEN ${TABLE}.campaign_source = 'direct'                                THEN 'Others'
        WHEN ${TABLE}.campaign_source IS NULL                                   THEN 'Unknown'
        ELSE 'Others'
      END
    ;;
  }

  ##############################################################
  # APP / SURFACE DIMENSIONS (Branch.io)
  ##############################################################
  dimension: device_os {
    type: string
    label: "Device OS"
    sql: ${TABLE}.device_os ;;
  }

  dimension: branch_channel { type: string label: "Branch Channel" sql: ${TABLE}.branch_channel ;; }
  dimension: branch_feature { type: string label: "Branch Feature" sql: ${TABLE}.branch_feature ;; }
  dimension: creative_name  { type: string label: "Creative Name"  sql: ${TABLE}.creative_name ;; }
  dimension: is_paid_branch { type: yesno  label: "Is Paid (Branch)" sql: ${TABLE}.is_paid_branch ;; }

  dimension: surface {
    type: string
    label: "Surface"
    sql:
      CASE
        WHEN ${TABLE}.event_type IN ('app_trial', 'app_install', 'app_reinstall') THEN 'app'
        ELSE                                                                           'web'
      END
    ;;
  }

  ##############################################################
  # TRIAL / LIFECYCLE DIMENSIONS
  ##############################################################
  dimension: order_id              { type: string sql: ${TABLE}.order_id ;; }
  dimension: conversion_event_type { type: string sql: ${TABLE}.conversion_event_type ;; }
  dimension: trial_type            { type: string sql: ${TABLE}.trial_type ;; }
  dimension: bundle_plan           { type: string sql: ${TABLE}.bundle_plan ;; }
  dimension: is_bundle_user        { type: yesno  sql: ${TABLE}.is_bundle_user ;; }

  dimension: lifecycle_event_type       { type: string sql: ${TABLE}.lifecycle_event_type ;; }
  dimension: is_activated_or_reacquired { type: yesno  sql: ${TABLE}.is_activated_or_reacquired ;; }
  dimension: is_activated               { type: yesno  sql: ${TABLE}.lifecycle_event_type = 'activation' ;; }
  dimension: is_not_retained            { type: yesno  sql: ${TABLE}.is_not_retained ;; }

  dimension: total_touches { type: number sql: ${TABLE}.total_touches ;; }

  dimension: days_before_conversion {
    type: number
    sql: ${TABLE}.min_days_before_conversion ;;
    hidden: yes
  }

  dimension: within_attribution_window {
    type: yesno
    sql:
      ${TABLE}.event_type IN ('page_visit', 'app_trial', 'app_install', 'app_reinstall')
      OR ${TABLE}.min_days_before_conversion IS NULL
      OR ${TABLE}.min_days_before_conversion <= {% parameter attribution_window_days %}
    ;;
    hidden: yes
  }

  ##############################################################
  # AOV DIMENSIONS
  ##############################################################
  dimension: activation_value {
    type: number
    label: "Activation Value"
    sql: ${TABLE}.activation_value ;;
    value_format_name: usd
  }

  dimension: is_yearly_plan { type: yesno   sql: ${TABLE}.is_yearly_plan ;; }
  dimension: plan_type      { type: string  sql: ${TABLE}.plan_type ;; }

  ##############################################################
  # ATTRIBUTION ROW METADATA
  ##############################################################
  dimension: credit_weight {
    type: number
    sql:
      CASE '{% parameter attribution_model %}'
           WHEN 'first_touch'    THEN ${TABLE}.credit_first_touch
           WHEN 'first_last'     THEN ${TABLE}.credit_first_last
           WHEN 'position_based' THEN ${TABLE}.credit_position_based
           ELSE                       ${TABLE}.credit_last_touch
      END
    ;;
    value_format_name: decimal_4
  }

  dimension: touch_position { type: string sql: ${TABLE}.touch_position ;; }

  dimension: is_primary_attribution {
    type: yesno
    sql:
      CASE '{% parameter attribution_model %}'
           WHEN 'first_touch'    THEN ${TABLE}.is_first_touch
           WHEN 'first_last'     THEN ${TABLE}.is_first_touch
           WHEN 'position_based' THEN ${TABLE}.is_first_touch
           ELSE                       ${TABLE}.is_last_touch
      END
    ;;
  }

  dimension: selected_attribution_model {
    type: string
    label: "Selected Attribution Model"
    sql:
      CASE '{% parameter attribution_model %}'
           WHEN 'first_touch'    THEN 'First Touch'
           WHEN 'first_last'     THEN 'First + Last Touch'
           WHEN 'position_based' THEN 'Position-Based (U-Shaped)'
           ELSE                       'Last Touch'
      END
    ;;
  }

  dimension: selected_attribution_window_days {
    type: number
    label: "Selected Attribution Window (Days)"
    sql: {% parameter attribution_window_days %} ;;
  }

  ##############################################################
  # QUALITY SCORE DIMENSIONS
  ##############################################################
  dimension: quality_score {
    type: number
    label: "Quality Score"
    sql: ${TABLE}.quality_score ;;
    value_format_name: decimal_2
  }

  dimension: volume_score { type: number sql: ${TABLE}.volume_score ;; value_format_name: decimal_2 }

  dimension: quality_grade {
    type: string
    label: "Quality Grade"
    sql:
      CASE
        WHEN ${TABLE}.quality_score >= 75 THEN 'A'
        WHEN ${TABLE}.quality_score >= 60 THEN 'B'
        WHEN ${TABLE}.quality_score >= 40 THEN 'C'
        WHEN ${TABLE}.quality_score >= 20 THEN 'D'
        WHEN ${TABLE}.quality_score IS NOT NULL THEN 'F'
        ELSE NULL
      END
    ;;
  }

  dimension: quality_tier {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.quality_score >= 60 THEN '1 — Top'
        WHEN ${TABLE}.quality_score >= 30 THEN '2 — Mid'
        WHEN ${TABLE}.quality_score IS NOT NULL THEN '3 — Low'
        ELSE NULL
      END
    ;;
  }

  dimension: trial_to_paid_rate { type: number label: "Trial → Paid Rate (Daily)" sql: ${TABLE}.trial_to_paid_rate ;; value_format_name: percent_2 }
  dimension: churn_rate         { type: number label: "Churn Rate (Daily)"        sql: ${TABLE}.churn_rate ;;        value_format_name: percent_2 }
  dimension: retention_rate     { type: number label: "Retention Rate (Daily)"    sql: ${TABLE}.retention_rate ;;    value_format_name: percent_2 }

  dimension: campaign_total_aov {
    type: number
    label: "Campaign Total Order Value (Daily)"
    sql: ${TABLE}.campaign_total_aov ;;
    value_format_name: usd_0
  }

  dimension: campaign_avg_aov {
    type: number
    label: "Campaign Avg Order Value (Daily)"
    sql: ${TABLE}.campaign_avg_aov ;;
    value_format_name: usd
  }

  dimension: std_trial_score    { type: number hidden: yes sql: ${TABLE}.std_trial_score ;; }
  dimension: bundle_trial_score { type: number hidden: yes sql: ${TABLE}.bundle_trial_score ;; }
  dimension: reacq_score        { type: number hidden: yes sql: ${TABLE}.reacq_score ;; }
  dimension: activation_score   { type: number hidden: yes sql: ${TABLE}.activation_score ;; }

  ##############################################################
  # MEASURES — Web KPIs
  ##############################################################
  measure: distinct_web_visits {
    type: count_distinct
    label: "Distinct Web Visits"
    sql: COALESCE(${TABLE}.user_id, ${TABLE}.anonymous_id) ;;
    filters: [event_type: "page_visit"]
    drill_fields: [drill_visits*]
  }

  measure: total_visits {
    type: count
    label: "Total Visits"
    filters: [event_type: "page_visit"]
  }

  measure: web_trials_started {
    type: count
    label: "Free Trials Started (Web)"
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"]
    drill_fields: [drill_conversions*]
  }

  measure: free_trials_started {
    type: count
    label: "Free Trials Started"
    description: "Alias for web trials. Use Total Trials Started for combined web+app."
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"]
    drill_fields: [drill_conversions*]
  }

  measure: free_trials_converted {
    type: count_distinct
    label: "Free Trials Converted"
    sql: ${TABLE}.user_id ;;
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_activated: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"]
    drill_fields: [drill_conversions*]
  }

  measure: reacquisitions {
    type: count
    label: "Reacquisitions"
    filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"]
    drill_fields: [drill_conversions*]
  }

  measure: trial_to_paid_conversion_rate {
    type: number
    label: "Trial to Paid Conversion Rate"
    sql: 1.0 * ${free_trials_converted} / NULLIF(${web_trials_started}, 0) ;;
    value_format_name: percent_2
  }

  measure: visit_to_trial_rate {
    type: number
    label: "Visit → Trial Rate"
    sql: 1.0 * ${web_trials_started} / NULLIF(${distinct_web_visits}, 0) ;;
    value_format_name: percent_2
  }

  ##############################################################
  # MEASURES — Branch.io App Trials, Installs & Reinstalls
  ##############################################################
  measure: app_trials_started {
    type: sum
    label: "Free Trials Started (App)"
    sql: ${TABLE}.app_trial_count ;;
    filters: [event_type: "app_trial"]
  }

  measure: app_trials_started_paid {
    type: sum
    label: "Free Trials Started (App, Paid Only)"
    sql: ${TABLE}.app_trial_count ;;
    filters: [event_type: "app_trial", is_paid_branch: "yes"]
  }

  measure: app_installs {
    type: sum
    label: "App Installs"
    sql: ${TABLE}.app_install_count ;;
    filters: [event_type: "app_install"]
  }

  measure: app_installs_paid {
    type: sum
    label: "App Installs (Paid Only)"
    sql: ${TABLE}.app_install_count ;;
    filters: [event_type: "app_install", is_paid_branch: "yes"]
  }

  measure: app_reinstalls {
    type: sum
    label: "App Reinstalls"
    sql: ${TABLE}.app_reinstall_count ;;
    filters: [event_type: "app_reinstall"]
  }

  measure: app_reinstalls_paid {
    type: sum
    label: "App Reinstalls (Paid Only)"
    sql: ${TABLE}.app_reinstall_count ;;
    filters: [event_type: "app_reinstall", is_paid_branch: "yes"]
  }

  measure: app_installs_and_reinstalls {
    type: number
    label: "App Installs + Reinstalls"
    sql: ${app_installs} + ${app_reinstalls} ;;
  }

  measure: app_reinstall_to_trial_rate {
    type: number
    label: "App Reinstall → Trial Rate"
    sql: 1.0 * ${app_trials_started} / NULLIF(${app_reinstalls}, 0) ;;
    value_format_name: percent_2
  }

  measure: total_trials_started {
    type: number
    label: "Free Trials Started (Web + App)"
    sql: ${web_trials_started} + ${app_trials_started} ;;
  }

  measure: app_share_of_trials {
    type: number
    label: "% App Trials"
    sql: 1.0 * ${app_trials_started} / NULLIF(${web_trials_started} + ${app_trials_started}, 0) ;;
    value_format_name: percent_2
  }

  measure: app_install_to_trial_rate {
    type: number
    label: "App Install → Trial Rate"
    sql: 1.0 * ${app_trials_started} / NULLIF(${app_installs}, 0) ;;
    value_format_name: percent_2
  }

  measure: app_install_to_trial_rate_paid {
    type: number
    label: "App Install → Trial Rate (Paid Only)"
    sql: 1.0 * ${app_trials_started_paid} / NULLIF(${app_installs_paid}, 0) ;;
    value_format_name: percent_2
  }

  ##############################################################
  # MEASURES — Average Touches
  ##############################################################
  measure: avg_touches_per_conversion {
    type: average
    label: "Avg Touches per Conversion"
    sql: ${TABLE}.total_touches ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"]
    value_format_name: decimal_2
  }

  measure: avg_touches_per_trial {
    type: average
    label: "Avg Touches per Free Trial"
    sql: ${TABLE}.total_touches ;;
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"]
    value_format_name: decimal_2
  }

  measure: avg_touches_per_reacquisition {
    type: average
    label: "Avg Touches per Reacquisition"
    sql: ${TABLE}.total_touches ;;
    filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"]
    value_format_name: decimal_2
  }

  measure: max_touches_per_conversion {
    type: max
    label: "Max Touches in a Journey"
    sql: ${TABLE}.total_touches ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: median_touches_per_conversion {
    type: median
    label: "Median Touches per Conversion"
    sql: ${TABLE}.total_touches ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"]
    value_format_name: decimal_1
  }

  ##############################################################
  # MEASURES — Retention
  ##############################################################
  measure: not_retained_users {
    type: count_distinct
    label: "Not Retained Users"
    sql: ${TABLE}.user_id ;;
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_not_retained: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: avg_retention_rate {
    type: average
    label: "Avg Retention Rate"
    sql: ${TABLE}.retention_rate ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes"]
    value_format_name: percent_2
  }

  measure: avg_churn_rate {
    type: average
    label: "Avg Churn Rate"
    sql: ${TABLE}.churn_rate ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes"]
    value_format_name: percent_2
  }

  ##############################################################
  # MEASURES — AOV
  ##############################################################
  measure: avg_order_value {
    type: average
    label: "Avg Order Value"
    sql: ${TABLE}.activation_value ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", activation_value: ">0", within_attribution_window: "yes"]
    value_format_name: usd
  }

  measure: total_order_value {
    type: sum
    label: "Total Order Value"
    sql: ${TABLE}.activation_value ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"]
    value_format_name: usd_0
  }

  measure: order_value_per_trial {
    type: number
    label: "Order Value per Trial"
    sql: 1.0 * ${total_order_value} / NULLIF(${web_trials_started}, 0) ;;
    value_format_name: usd
  }

  measure: yearly_plan_users {
    type: count_distinct
    label: "Yearly Plan Users"
    sql: ${TABLE}.user_id ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", is_yearly_plan: "yes", within_attribution_window: "yes"]
  }

  measure: monthly_plan_users {
    type: count_distinct
    label: "Monthly Plan Users"
    sql: ${TABLE}.user_id ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", is_yearly_plan: "no", activation_value: ">0", within_attribution_window: "yes"]
  }

  measure: pct_yearly_plan {
    type: number
    label: "% Yearly Plan Subs"
    sql: 1.0 * ${yearly_plan_users} / NULLIF(${yearly_plan_users} + ${monthly_plan_users}, 0) ;;
    value_format_name: percent_2
  }

  measure: pct_monthly_plan {
    type: number
    label: "% Monthly Plan Subs"
    sql: 1.0 * ${monthly_plan_users} / NULLIF(${yearly_plan_users} + ${monthly_plan_users}, 0) ;;
    value_format_name: percent_2
  }

  ##############################################################
  # MEASURES — Quality Score
  ##############################################################
  measure: avg_quality_score {
    type: average
    label: "Avg Quality Score"
    sql: ${TABLE}.quality_score ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes"]
    value_format_name: decimal_2
    drill_fields: [drill_quality*]
  }

  measure: max_quality_score {
    type: max
    label: "Best Day Quality Score"
    sql: ${TABLE}.quality_score ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes"]
    value_format_name: decimal_2
  }

  measure: top_grade_days {
    type: count_distinct
    label: "Days Graded A or B"
    sql: ${TABLE}.report_date ;;
    filters: [event_type: "conversion", is_primary_attribution: "yes", quality_grade: "A,B"]
  }

  ##############################################################
  # MEASURES — Detail breakouts
  ##############################################################
  measure: total_converted {
    type: count_distinct
    label: "Total Converted"
    sql: ${TABLE}.user_id ;;
    filters: [event_type: "conversion", conversion_event_type: "free_trial", is_activated: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: total_reacquisition {
    type: count
    label: "Total Reacquisition"
    filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: standard_trials {
    type: count
    label: "Standard Trials"
    filters: [event_type: "conversion", conversion_event_type: "free_trial", trial_type: "standard", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: bundle_trials {
    type: count
    label: "Bundle Trials"
    filters: [event_type: "conversion", conversion_event_type: "free_trial", trial_type: "bundle", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  measure: total_conversions {
    type: count
    label: "Total Conversions"
    filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"]
  }

  ##############################################################
  # MEASURES — Credit-weighted
  ##############################################################
  measure: free_trials_started_weighted {
    type: sum
    label: "Free Trials Started (Credit-Weighted)"
    sql: ${credit_weight} ;;
    filters: [event_type: "conversion", conversion_event_type: "free_trial", within_attribution_window: "yes"]
    value_format_name: decimal_4
  }

  measure: reacquisitions_weighted {
    type: sum
    label: "Reacquisitions (Credit-Weighted)"
    sql: ${credit_weight} ;;
    filters: [event_type: "conversion", conversion_event_type: "reacquisition", within_attribution_window: "yes"]
    value_format_name: decimal_4
  }

  ##############################################################
  # MEASURES — Touch position breakouts
  ##############################################################
  measure: first_touch_credit {
    type: sum
    label: "First Touch Credit"
    sql: ${credit_weight} ;;
    filters: [event_type: "conversion", touch_position: "first", within_attribution_window: "yes"]
    value_format_name: decimal_4
  }

  measure: middle_touch_credit {
    type: sum
    label: "Middle Touch Credit"
    sql: ${credit_weight} ;;
    filters: [event_type: "conversion", touch_position: "middle", within_attribution_window: "yes"]
    value_format_name: decimal_4
  }

  measure: last_touch_credit {
    type: sum
    label: "Last Touch Credit"
    sql: ${credit_weight} ;;
    filters: [event_type: "conversion", touch_position: "last", within_attribution_window: "yes"]
    value_format_name: decimal_4
  }

  ##############################################################
  # DRILL FIELD SETS
  ##############################################################
  set: drill_visits {
    fields: [
      report_date_date, marketing_platform, campaign_source, campaign_medium,
      campaign_name, total_visits, distinct_web_visits
    ]
  }

  set: drill_conversions {
    fields: [
      report_date_date, user_id, order_id, conversion_event_type, trial_type,
      plan_type, activation_value, surface, device_os, marketing_platform,
      campaign_source, campaign_medium, campaign_name, campaign_content,
      lifecycle_event_type, lifecycle_event_date, is_not_retained,
      total_touches, touch_position, credit_weight, quality_score, quality_grade
    ]
  }

  set: drill_quality {
    fields: [
      report_date_date, campaign_name, campaign_medium, campaign_content,
      marketing_platform, surface, standard_trials, bundle_trials, reacquisitions,
      total_converted, not_retained_users, avg_touches_per_conversion,
      retention_rate, trial_to_paid_rate, avg_order_value, pct_yearly_plan,
      pct_monthly_plan, quality_score, quality_grade
    ]
  }

  set: campaign_subscription_events_set {
    fields: [
      campaign_name, campaign_content, marketing_platform, surface, device_os,
      campaign_medium, web_trials_started, app_trials_started, app_installs,
      app_reinstalls, app_installs_and_reinstalls,
      total_trials_started, app_share_of_trials, app_install_to_trial_rate,
      app_reinstall_to_trial_rate,
      free_trials_started_weighted, total_converted, total_reacquisition,
      avg_touches_per_trial, avg_order_value, pct_yearly_plan, pct_monthly_plan,
      avg_quality_score, quality_grade
    ]
  }
}

################################################################################
# Datagroup — triggers the daily incremental run at 1 AM ET
################################################################################
datagroup: marketing_attribution_daily {
 sql_trigger:SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '7 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
