view: marketing_attribution_test {
  derived_table: {

    increment_key: "report_date"
    increment_offset: 7
    datagroup_trigger: marketing_attribution_daily
    distribution_style: even
    indexes: ["report_date", "event_type", "campaign_source", "user_id"]

    sql:
      SELECT * FROM (

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
                ,CAST(NULL AS NUMERIC(10,2)) AS quality_score
                ,CAST(NULL AS NUMERIC(10,2)) AS volume_score
                ,CAST(NULL AS NUMERIC(5,4))  AS trial_to_paid_rate
                ,CAST(NULL AS NUMERIC(5,4))  AS churn_rate
                ,CAST(NULL AS NUMERIC(5,4))  AS retention_rate
                ,CAST(NULL AS NUMERIC(12,2)) AS campaign_total_aov
                ,CAST(NULL AS NUMERIC(10,2)) AS campaign_avg_aov
                ,CAST(NULL AS NUMERIC(10,2)) AS std_trial_score
                ,CAST(NULL AS NUMERIC(10,2)) AS bundle_trial_score
                ,CAST(NULL AS NUMERIC(10,2)) AS reacq_score
                ,CAST(NULL AS NUMERIC(10,2)) AS activation_score
              FROM (
                SELECT
                  context_ip, anonymous_id
                  ,event_received_at AS received_at
                  ,event_date
                  ,COALESCE(campaign_source, 'organic') AS campaign_source
                  ,COALESCE(campaign_name,   'organic') AS campaign_name
                  ,campaign_id
                  ,COALESCE(campaign_medium, 'organic') AS campaign_medium
                  ,campaign_content
                  ,event_id AS touch_event_id
                  ,user_id
                FROM (
                  SELECT
                    user_id, context_ip, anonymous_id
                    ,received_at AS event_received_at
                    ,DATE(received_at) AS event_date
                    ,id AS event_id
                    ,context_campaign_source AS campaign_source
                    ,context_campaign_name   AS campaign_name
                    ,context_campaign_id     AS campaign_id
                    ,context_campaign_medium AS campaign_medium
                    ,context_campaign_content AS campaign_content
                  FROM javascript_upff_home.pages
                  WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE
                    AND received_at <  CURRENT_DATE + INTERVAL '1 day'
                ) page_src
              ) marketing_touches

      UNION ALL

      SELECT
      CAST('conversion' AS VARCHAR(20)) AS event_type
      ,sa.order_id AS event_id
      ,sa.user_id
      ,CAST(NULL AS VARCHAR(255)) AS context_ip
      ,CAST(NULL AS VARCHAR(255)) AS anonymous_id
      ,sa.conversion_event_at     AS event_at
      ,DATE(sa.conversion_event_at) AS report_date
      ,sa.attributed_campaign_source  AS campaign_source
      ,sa.attributed_campaign_name    AS campaign_name
      ,sa.attributed_campaign_id      AS campaign_id
      ,sa.attributed_campaign_medium  AS campaign_medium
      ,sa.attributed_campaign_content AS campaign_content
      ,sa.order_id, sa.conversion_event_type
      ,sa.trial_type, sa.bundle_plan, sa.is_bundle_user
      ,sa.lifecycle_event_type, sa.lifecycle_event_date
      ,sa.is_activated_or_reacquired, sa.is_not_retained
      ,CAST(sa.activation_value AS NUMERIC(10,2)) AS activation_value
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
      FROM (
      SELECT
      order_id, user_id, conversion_event_at, conversion_event_type
      ,trial_type, bundle_plan, is_bundle_user
      ,lifecycle_event_date, lifecycle_event_type
      ,is_activated_or_reacquired, is_not_retained
      ,activation_value, is_yearly_plan, plan_type
      ,total_touches, min_days_before_conversion, attributed_touch_at
      ,attributed_campaign_source, attributed_campaign_name, attributed_campaign_id
      ,attributed_campaign_medium, attributed_campaign_content
      ,credit_first_touch, credit_last_touch, credit_first_last, credit_position_based
      ,touch_position, is_first_touch, is_last_touch, is_first_and_last
      FROM (
      SELECT
      order_id, user_id, conversion_event_at, conversion_event_type
      ,trial_type, bundle_plan, is_bundle_user
      ,lifecycle_event_date, lifecycle_event_type
      ,is_activated_or_reacquired, is_not_retained
      ,activation_value, is_yearly_plan, plan_type, total_touches
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
      ,BOOL_OR(last_touch_rank  = 1)       AS is_last_touch
      ,BOOL_OR(is_first_and_last)          AS is_first_and_last
      FROM (
      SELECT
      at2.order_id, at2.user_id
      ,at2.conversion_event_at, at2.conversion_event_type
      ,at2.trial_type, at2.bundle_plan, at2.is_bundle_user
      ,at2.lifecycle_event_date, at2.lifecycle_event_type
      ,at2.is_activated_or_reacquired, at2.is_not_retained
      ,at2.activation_value, at2.is_yearly_plan, at2.plan_type
      ,at2.total_touches
      ,at2.touch_received_at AS attributed_touch_at
      ,at2.days_before_conversion
      ,CAST(at2.campaign_source  AS VARCHAR(255)) AS attributed_campaign_source
      ,CAST(at2.campaign_name    AS VARCHAR(255)) AS attributed_campaign_name
      ,CAST(at2.campaign_id      AS VARCHAR(255)) AS attributed_campaign_id
      ,CAST(at2.campaign_medium  AS VARCHAR(255)) AS attributed_campaign_medium
      ,CAST(at2.campaign_content AS VARCHAR(255)) AS attributed_campaign_content
      ,at2.first_touch_rank, at2.last_touch_rank
      ,CAST(CASE WHEN at2.first_touch_rank = 1 THEN 1.0 ELSE 0.0 END AS NUMERIC(7,6)) AS credit_first_touch
      ,CAST(CASE WHEN at2.last_touch_rank  = 1 THEN 1.0 ELSE 0.0 END AS NUMERIC(7,6)) AS credit_last_touch
      ,CAST(CASE
      WHEN flm.is_first_and_last AND at2.first_touch_rank = 1 THEN 1.0
      WHEN flm.is_first_and_last                               THEN 0.0
      WHEN at2.first_touch_rank = 1                            THEN 0.5
      WHEN at2.last_touch_rank  = 1                            THEN 0.5
      ELSE 0.0
      END AS NUMERIC(7,6)) AS credit_first_last
      ,CAST(CASE
      WHEN at2.total_touches = 1                                THEN 1.0
      WHEN at2.total_touches = 2 AND at2.first_touch_rank = 1   THEN 0.5
      WHEN at2.total_touches = 2 AND at2.last_touch_rank  = 1   THEN 0.5
      WHEN at2.first_touch_rank = 1                             THEN 0.4
      WHEN at2.last_touch_rank  = 1                             THEN 0.4
      ELSE COALESCE(0.2 / NULLIF(at2.total_touches - 2, 0), 0)
      END AS NUMERIC(7,6)) AS credit_position_based
      ,CAST(CASE
      WHEN at2.total_touches = 1    THEN 'only'
      WHEN at2.first_touch_rank = 1 THEN 'first'
      WHEN at2.last_touch_rank  = 1 THEN 'last'
      ELSE 'middle'
      END AS VARCHAR(20)) AS touch_position
      ,COALESCE(flm.is_first_and_last, FALSE) AS is_first_and_last
      FROM (
      SELECT
      c.order_id, c.user_id
      ,c.conversion_event_at, c.conversion_event_type
      ,c.trial_type, c.bundle_plan, c.is_bundle_user
      ,c.lifecycle_event_date, c.lifecycle_event_type
      ,c.is_activated_or_reacquired, c.is_not_retained
      ,c.activation_value, c.is_yearly_plan, c.plan_type
      ,mt.received_at AS touch_received_at
      ,mt.campaign_source, mt.campaign_name, mt.campaign_id
      ,mt.campaign_medium, mt.campaign_content
      ,DATEDIFF(day, mt.received_at, c.conversion_event_at) AS days_before_conversion
      ,ROW_NUMBER() OVER (PARTITION BY c.conversion_event_type, c.order_id ORDER BY mt.received_at ASC)  AS first_touch_rank
      ,ROW_NUMBER() OVER (PARTITION BY c.conversion_event_type, c.order_id ORDER BY mt.received_at DESC) AS last_touch_rank
      ,COUNT(*) OVER    (PARTITION BY c.conversion_event_type, c.order_id)                               AS total_touches
      FROM (
      SELECT
      r.user_id, r.context_ip, r.anonymous_id, r.order_id
      ,r.event_received_at AS conversion_event_at
      ,'free_trial'        AS conversion_event_type
      ,r.trial_type, r.bundle_plan
      ,CASE WHEN r.bundle_plan IS NOT NULL THEN TRUE ELSE FALSE END AS is_bundle_user
      ,ulf.activation_date, ulf.reacquisition_date
      ,COALESCE(ulf.is_not_retained, FALSE) AS is_not_retained
      ,ulf.not_retained_date
      ,CASE
      WHEN ulf.activation_date IS NOT NULL AND ulf.activation_date >= DATE(r.event_received_at) THEN 'activation'
      WHEN ulf.reacquisition_date IS NOT NULL THEN 'reacquisition'
      ELSE NULL
      END AS lifecycle_event_type
      ,CASE
      WHEN ulf.activation_date IS NOT NULL AND ulf.activation_date >= DATE(r.event_received_at) THEN ulf.activation_date
      ELSE ulf.reacquisition_date
      END AS lifecycle_event_date
      ,CASE
      WHEN (ulf.activation_date IS NOT NULL AND ulf.activation_date >= DATE(r.event_received_at)) OR ulf.reacquisition_date IS NOT NULL THEN TRUE
      ELSE FALSE
      END AS is_activated_or_reacquired
      ,COALESCE(uav.activation_value_dollars, 0) AS activation_value
      ,COALESCE(uav.is_yearly_plan, FALSE)       AS is_yearly_plan
      ,COALESCE(uav.plan_type, 'unknown')        AS plan_type
      FROM (
      SELECT user_id, context_ip, anonymous_id, order_id, event_received_at, trial_type, bundle_plan
      ,ROW_NUMBER() OVER (PARTITION BY user_id, order_id ORDER BY CASE WHEN bundle_plan IS NOT NULL THEN 0 ELSE 1 END, event_received_at ASC) AS dedup_rank
      FROM (
      SELECT user_id, context_ip, anonymous_id, received_at AS event_received_at, order_id, bundle_plan, CAST('standard' AS VARCHAR(20)) AS trial_type FROM javaScript_upentertainment_checkout.order_completed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND received_at < CURRENT_DATE + INTERVAL '1 day' AND user_id IS NOT NULL AND brand = 'upfaithandfamily'
      UNION ALL
      SELECT user_id, context_ip, anonymous_id, received_at, order_id, bundle_plan, CAST('bundle' AS VARCHAR(20)) FROM javaScript_upentertainment_checkout.order_updated WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND received_at < CURRENT_DATE + INTERVAL '1 day' AND user_id IS NOT NULL AND brand = 'upfaithandfamily' AND bundle_plan IS NOT NULL
      ) le_ft
      ) r
      LEFT JOIN (
      SELECT user_id
      ,MIN(CASE WHEN src = 'activation'    THEN edate END) AS activation_date
      ,MIN(CASE WHEN src = 'reacquisition' THEN edate END) AS reacquisition_date
      ,MIN(CASE WHEN src = 'not_retained'  THEN edate END) AS not_retained_date
      ,CAST(MAX(CASE WHEN src = 'not_retained' THEN 1 ELSE 0 END) AS BOOLEAN) AS is_not_retained
      FROM (
      SELECT user_id, DATE(received_at) AS edate, 'activation' AS src FROM chargebee_webhook_events.subscription_activated WHERE DATE(received_at) BETWEEN (CURRENT_DATE - INTERVAL '90 days')::DATE AND CURRENT_DATE AND content_subscription_subscription_items LIKE '%UP%' AND user_id IS NOT NULL
      UNION ALL
      SELECT user_id, DATE(received_at), 'reacquisition' FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand = 'upfaithandfamily'
      UNION ALL
      SELECT user_id, DATE(received_at), 'not_retained' FROM (SELECT user_id, received_at, id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY received_at ASC) AS rn FROM chargebee_webhook_events.subscription_cancelled WHERE DATE(received_at) BETWEEN (CURRENT_DATE - INTERVAL '90 days')::DATE AND CURRENT_DATE AND content_subscription_subscription_items LIKE '%UP%' AND content_subscription_cancelled_at IS NOT NULL AND content_subscription_trial_end IS NOT NULL AND (content_subscription_cancelled_at - content_subscription_trial_end < 10000) AND user_id IS NOT NULL) cx WHERE rn = 1
      ) lifecycle_src
      GROUP BY user_id
      ) ulf ON r.user_id = ulf.user_id
      LEFT JOIN (
      SELECT user_id, activation_value_cents / 100.0 AS activation_value_dollars
      ,CASE WHEN activation_value_cents >= 5000 THEN 'yearly' WHEN activation_value_cents > 0 THEN 'monthly' ELSE 'unknown' END AS plan_type
      ,CASE WHEN activation_value_cents >= 5000 THEN TRUE ELSE FALSE END AS is_yearly_plan
      FROM (SELECT user_id, content_subscription_subscription_items_0_unit_price AS activation_value_cents, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY received_at ASC) AS rn FROM chargebee_webhook_events.subscription_activated WHERE DATE(received_at) BETWEEN (CURRENT_DATE - INTERVAL '90 days')::DATE AND CURRENT_DATE AND content_subscription_subscription_items LIKE '%UP%' AND content_subscription_subscription_items_0_unit_price IS NOT NULL AND content_subscription_subscription_items_0_unit_price > 0 AND user_id IS NOT NULL) ranked WHERE rn = 1
      ) uav ON r.user_id = uav.user_id
      WHERE r.dedup_rank = 1

      UNION ALL

      SELECT
      le3.user_id, le3.context_ip, le3.anonymous_id
      ,le3.event_id AS order_id
      ,le3.received_at AS conversion_event_at
      ,'reacquisition' AS conversion_event_type
      ,CAST(NULL AS VARCHAR(20)) AS trial_type, CAST(NULL AS VARCHAR(255)) AS bundle_plan, FALSE AS is_bundle_user
      ,CAST(NULL AS DATE) AS activation_date, DATE(le3.received_at) AS reacquisition_date
      ,FALSE AS is_not_retained, CAST(NULL AS DATE) AS not_retained_date
      ,'reacquisition' AS lifecycle_event_type, DATE(le3.received_at) AS lifecycle_event_date
      ,TRUE AS is_activated_or_reacquired
      ,COALESCE(uav2.activation_value_dollars, 0) AS activation_value
      ,COALESCE(uav2.is_yearly_plan, FALSE) AS is_yearly_plan
      ,COALESCE(uav2.plan_type, 'unknown') AS plan_type
      FROM (
      SELECT user_id, context_ip, anonymous_id, id AS event_id, received_at FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND received_at < CURRENT_DATE + INTERVAL '1 day' AND user_id IS NOT NULL AND brand = 'upfaithandfamily'
      ) le3
      LEFT JOIN (
      SELECT user_id, activation_value_cents / 100.0 AS activation_value_dollars
      ,CASE WHEN activation_value_cents >= 5000 THEN 'yearly' WHEN activation_value_cents > 0 THEN 'monthly' ELSE 'unknown' END AS plan_type
      ,CASE WHEN activation_value_cents >= 5000 THEN TRUE ELSE FALSE END AS is_yearly_plan
      FROM (SELECT user_id, content_subscription_subscription_items_0_unit_price AS activation_value_cents, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY received_at ASC) AS rn FROM chargebee_webhook_events.subscription_activated WHERE DATE(received_at) BETWEEN (CURRENT_DATE - INTERVAL '90 days')::DATE AND CURRENT_DATE AND content_subscription_subscription_items LIKE '%UP%' AND content_subscription_subscription_items_0_unit_price IS NOT NULL AND content_subscription_subscription_items_0_unit_price > 0 AND user_id IS NOT NULL) ranked2 WHERE rn = 1
      ) uav2 ON le3.user_id = uav2.user_id
      ) c
      JOIN (
      SELECT context_ip, anonymous_id, received_at
      ,COALESCE(context_campaign_source, 'organic') AS campaign_source
      ,COALESCE(context_campaign_name,   'organic') AS campaign_name
      ,context_campaign_id AS campaign_id
      ,COALESCE(context_campaign_medium, 'organic') AS campaign_medium
      ,context_campaign_content AS campaign_content
      FROM javascript_upff_home.pages
      WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE
      AND received_at <  CURRENT_DATE + INTERVAL '1 day'
      ) mt
      ON (mt.anonymous_id = c.anonymous_id OR mt.context_ip = c.context_ip)
      AND mt.received_at <= c.conversion_event_at
      AND mt.received_at >= c.conversion_event_at - (90 || ' days')::INTERVAL
      ) at2
      LEFT JOIN (
      SELECT f2.order_id, f2.conversion_event_type
      ,CASE WHEN COALESCE(f2.cs,'')=COALESCE(l2.cs,'') AND COALESCE(f2.cm,'')=COALESCE(l2.cm,'') AND COALESCE(f2.cn,'')=COALESCE(l2.cn,'') THEN TRUE ELSE FALSE END AS is_first_and_last
      FROM (
      SELECT order_id, conversion_event_type, campaign_source AS cs, campaign_medium AS cm, campaign_name AS cn
      FROM (SELECT c2.order_id, c2.conversion_event_type, mt2.campaign_source, mt2.campaign_name, mt2.campaign_medium, ROW_NUMBER() OVER (PARTITION BY c2.conversion_event_type, c2.order_id ORDER BY mt2.received_at ASC) AS rk FROM (SELECT user_id, context_ip, anonymous_id, order_id, received_at AS conversion_event_at, 'free_trial' AS conversion_event_type FROM javaScript_upentertainment_checkout.order_completed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT user_id, context_ip, anonymous_id, id, received_at, 'reacquisition' FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily') c2 JOIN (SELECT context_ip, anonymous_id, received_at, COALESCE(context_campaign_source,'organic') AS campaign_source, COALESCE(context_campaign_name,'organic') AS campaign_name, COALESCE(context_campaign_medium,'organic') AS campaign_medium FROM javascript_upff_home.pages WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE) mt2 ON (mt2.anonymous_id=c2.anonymous_id OR mt2.context_ip=c2.context_ip) AND mt2.received_at <= c2.conversion_event_at AND mt2.received_at >= c2.conversion_event_at - (90||' days')::INTERVAL) x WHERE rk=1
      ) f2
      JOIN (
      SELECT order_id, conversion_event_type, campaign_source AS cs, campaign_medium AS cm, campaign_name AS cn
      FROM (SELECT c2.order_id, c2.conversion_event_type, mt2.campaign_source, mt2.campaign_name, mt2.campaign_medium, ROW_NUMBER() OVER (PARTITION BY c2.conversion_event_type, c2.order_id ORDER BY mt2.received_at DESC) AS rk FROM (SELECT user_id, context_ip, anonymous_id, order_id, received_at AS conversion_event_at, 'free_trial' AS conversion_event_type FROM javaScript_upentertainment_checkout.order_completed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT user_id, context_ip, anonymous_id, id, received_at, 'reacquisition' FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily') c2 JOIN (SELECT context_ip, anonymous_id, received_at, COALESCE(context_campaign_source,'organic') AS campaign_source, COALESCE(context_campaign_name,'organic') AS campaign_name, COALESCE(context_campaign_medium,'organic') AS campaign_medium FROM javascript_upff_home.pages WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE) mt2 ON (mt2.anonymous_id=c2.anonymous_id OR mt2.context_ip=c2.context_ip) AND mt2.received_at <= c2.conversion_event_at AND mt2.received_at >= c2.conversion_event_at - (90||' days')::INTERVAL) x WHERE rk=1
      ) l2 ON f2.order_id=l2.order_id AND f2.conversion_event_type=l2.conversion_event_type
      ) flm ON at2.order_id=flm.order_id AND at2.conversion_event_type=flm.conversion_event_type
      ) atr
      GROUP BY order_id, user_id, conversion_event_at, conversion_event_type, trial_type, bundle_plan, is_bundle_user, lifecycle_event_date, lifecycle_event_type, is_activated_or_reacquired, is_not_retained, activation_value, is_yearly_plan, plan_type, total_touches, attributed_campaign_source, attributed_campaign_name, attributed_campaign_medium

      UNION ALL

      SELECT
      c3.order_id, c3.user_id, c3.conversion_event_at, c3.conversion_event_type
      ,c3.trial_type, c3.bundle_plan, c3.is_bundle_user
      ,c3.lifecycle_event_date, c3.lifecycle_event_type
      ,c3.is_activated_or_reacquired, c3.is_not_retained
      ,c3.activation_value, c3.is_yearly_plan, c3.plan_type
      ,0 AS total_touches, CAST(NULL AS INTEGER) AS min_days_before_conversion
      ,CAST(NULL AS TIMESTAMP) AS attributed_touch_at
      ,CAST('direct' AS VARCHAR(255)) AS attributed_campaign_source
      ,CAST('direct' AS VARCHAR(255)) AS attributed_campaign_name
      ,CAST(NULL AS VARCHAR(255)) AS attributed_campaign_id
      ,CAST('direct' AS VARCHAR(255)) AS attributed_campaign_medium
      ,CAST(NULL AS VARCHAR(255)) AS attributed_campaign_content
      ,CAST(1.0 AS NUMERIC(7,6)) AS credit_first_touch
      ,CAST(1.0 AS NUMERIC(7,6)) AS credit_last_touch
      ,CAST(1.0 AS NUMERIC(7,6)) AS credit_first_last
      ,CAST(1.0 AS NUMERIC(7,6)) AS credit_position_based
      ,CAST('only' AS VARCHAR(20)) AS touch_position
      ,TRUE AS is_first_touch, TRUE AS is_last_touch, TRUE AS is_first_and_last
      FROM (
      SELECT r3.user_id, r3.context_ip, r3.anonymous_id, r3.order_id
      ,r3.event_received_at AS conversion_event_at, 'free_trial' AS conversion_event_type
      ,r3.trial_type, r3.bundle_plan
      ,CASE WHEN r3.bundle_plan IS NOT NULL THEN TRUE ELSE FALSE END AS is_bundle_user
      ,CAST(NULL AS DATE) AS activation_date, CAST(NULL AS DATE) AS reacquisition_date
      ,FALSE AS is_not_retained, CAST(NULL AS DATE) AS not_retained_date
      ,CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type, CAST(NULL AS DATE) AS lifecycle_event_date
      ,FALSE AS is_activated_or_reacquired
      ,CAST(0 AS NUMERIC(10,2)) AS activation_value, FALSE AS is_yearly_plan, 'unknown' AS plan_type
      FROM (SELECT user_id, context_ip, anonymous_id, order_id, received_at AS event_received_at, bundle_plan, CAST('standard' AS VARCHAR(20)) AS trial_type, ROW_NUMBER() OVER (PARTITION BY user_id, order_id ORDER BY CASE WHEN bundle_plan IS NOT NULL THEN 0 ELSE 1 END, received_at ASC) AS dedup_rank FROM (SELECT user_id, context_ip, anonymous_id, received_at, order_id, bundle_plan FROM javaScript_upentertainment_checkout.order_completed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT user_id, context_ip, anonymous_id, received_at, order_id, bundle_plan FROM javaScript_upentertainment_checkout.order_updated WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' AND bundle_plan IS NOT NULL) le_dc) r3 WHERE r3.dedup_rank = 1
      UNION ALL
      SELECT user_id, context_ip, anonymous_id, id AS order_id, received_at AS conversion_event_at, 'reacquisition' AS conversion_event_type, CAST(NULL AS VARCHAR(20)) AS trial_type, CAST(NULL AS VARCHAR(255)) AS bundle_plan, FALSE AS is_bundle_user, CAST(NULL AS DATE), DATE(received_at), FALSE, CAST(NULL AS DATE), 'reacquisition', DATE(received_at), TRUE, CAST(0 AS NUMERIC(10,2)), FALSE, 'unknown' FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND received_at < CURRENT_DATE + INTERVAL '1 day' AND user_id IS NOT NULL AND brand='upfaithandfamily'
      ) c3
      LEFT JOIN (
      SELECT DISTINCT c4.order_id, c4.conversion_event_type
      FROM (SELECT order_id, received_at AS conversion_event_at, 'free_trial' AS conversion_event_type, context_ip, anonymous_id FROM javaScript_upentertainment_checkout.order_completed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT id, received_at, 'reacquisition', context_ip, anonymous_id FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily') c4
      JOIN (SELECT context_ip, anonymous_id, received_at FROM javascript_upff_home.pages WHERE received_at >= (CURRENT_DATE - INTERVAL '90 days')::DATE) mt3
      ON (mt3.anonymous_id=c4.anonymous_id OR mt3.context_ip=c4.context_ip) AND mt3.received_at <= c4.conversion_event_at AND mt3.received_at >= c4.conversion_event_at - (90||' days')::INTERVAL
      ) at3 ON c3.order_id=at3.order_id AND c3.conversion_event_type=at3.conversion_event_type
      WHERE at3.order_id IS NULL
      ) sa_all
      ) sa
      LEFT JOIN (
      SELECT
      m2.report_date, m2.campaign_name, m2.campaign_medium, m2.campaign_content
      ,CAST(COALESCE(m2.total_order_value,0) AS NUMERIC(12,2)) AS total_order_value
      ,CAST(COALESCE(m2.avg_order_value,0)   AS NUMERIC(10,2)) AS avg_order_value
      ,CAST(CASE WHEN m2.unique_trial_users > 0 THEN 1.0*m2.activations/m2.unique_trial_users ELSE 0 END AS NUMERIC(5,4)) AS trial_to_paid_rate
      ,CAST(COALESCE(m2.churn_rate_raw,0)       AS NUMERIC(5,4)) AS churn_rate
      ,CAST(1.0-COALESCE(m2.churn_rate_raw,0)   AS NUMERIC(5,4)) AS retention_rate
      ,CAST(100.0*m2.standard_trials/GREATEST(n2.max_standard,1)   AS NUMERIC(10,4)) AS std_trial_score
      ,CAST(100.0*m2.bundle_trials/GREATEST(n2.max_bundle,1)       AS NUMERIC(10,4)) AS bundle_trial_score
      ,CAST(100.0*m2.reacquisitions/GREATEST(n2.max_reacq,1)       AS NUMERIC(10,4)) AS reacq_score
      ,CAST(100.0*m2.activations/GREATEST(n2.max_activations,1)    AS NUMERIC(10,4)) AS activation_score
      ,CAST(0.15*(100.0*m2.standard_trials/GREATEST(n2.max_standard,1))+0.20*(100.0*m2.bundle_trials/GREATEST(n2.max_bundle,1))+0.25*(100.0*m2.reacquisitions/GREATEST(n2.max_reacq,1))+0.40*(100.0*m2.activations/GREATEST(n2.max_activations,1)) AS NUMERIC(10,4)) AS volume_score
      ,CAST(CAST(0.15*(100.0*m2.standard_trials/GREATEST(n2.max_standard,1))+0.20*(100.0*m2.bundle_trials/GREATEST(n2.max_bundle,1))+0.25*(100.0*m2.reacquisitions/GREATEST(n2.max_reacq,1))+0.40*(100.0*m2.activations/GREATEST(n2.max_activations,1)) AS NUMERIC(10,4))*CAST(0.5+LEAST(CASE WHEN m2.unique_trial_users>0 THEN 1.0*m2.activations/m2.unique_trial_users ELSE 0 END/0.50,1.0) AS NUMERIC(5,4))*CAST(GREATEST(1.0-COALESCE(m2.churn_rate_raw,0),0.0) AS NUMERIC(5,4)) AS NUMERIC(10,4)) AS quality_score
      FROM (
      SELECT DATE(sa4.conversion_event_at) AS report_date, sa4.attributed_campaign_name AS campaign_name, sa4.attributed_campaign_medium AS campaign_medium, sa4.attributed_campaign_content AS campaign_content
      ,SUM(CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.trial_type='standard' AND sa4.is_last_touch THEN 1 ELSE 0 END) AS standard_trials
      ,SUM(CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.trial_type='bundle'   AND sa4.is_last_touch THEN 1 ELSE 0 END) AS bundle_trials
      ,SUM(CASE WHEN sa4.conversion_event_type='reacquisition' AND sa4.is_last_touch THEN 1 ELSE 0 END) AS reacquisitions
      ,COUNT(DISTINCT CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.lifecycle_event_type='activation' AND sa4.is_last_touch THEN sa4.user_id END) AS activations
      ,COUNT(DISTINCT CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.is_last_touch THEN sa4.user_id END) AS unique_trial_users
      ,COUNT(DISTINCT CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.is_not_retained AND sa4.is_last_touch THEN sa4.user_id END)*1.0/NULLIF(COUNT(DISTINCT CASE WHEN sa4.conversion_event_type='free_trial' AND sa4.is_last_touch THEN sa4.user_id END),0) AS churn_rate_raw
      ,SUM(CASE WHEN sa4.is_last_touch AND sa4.activation_value>0 THEN sa4.activation_value ELSE 0 END) AS total_order_value
      ,AVG(CASE WHEN sa4.is_last_touch AND sa4.activation_value>0 THEN sa4.activation_value END) AS avg_order_value
      FROM (
      SELECT order_id,user_id,conversion_event_at,conversion_event_type,trial_type,lifecycle_event_type,activation_value,is_not_retained,attributed_campaign_name,attributed_campaign_medium,attributed_campaign_content,BOOL_OR(last_touch_rank=1) AS is_last_touch
      FROM (SELECT at2.order_id,at2.user_id,at2.conversion_event_at,at2.conversion_event_type,at2.trial_type,at2.lifecycle_event_type,at2.activation_value,at2.is_not_retained,CAST(at2.campaign_name AS VARCHAR(255)) AS attributed_campaign_name,CAST(at2.campaign_medium AS VARCHAR(255)) AS attributed_campaign_medium,CAST(at2.campaign_content AS VARCHAR(255)) AS attributed_campaign_content,at2.last_touch_rank FROM (SELECT c.order_id,c.user_id,c.conversion_event_at,c.conversion_event_type,c.trial_type,c.lifecycle_event_type,c.activation_value,c.is_not_retained,mt.campaign_name,mt.campaign_medium,mt.campaign_content,ROW_NUMBER() OVER (PARTITION BY c.conversion_event_type,c.order_id ORDER BY mt.received_at DESC) AS last_touch_rank FROM (SELECT r.user_id,r.context_ip,r.anonymous_id,r.order_id,r.event_received_at AS conversion_event_at,'free_trial' AS conversion_event_type,r.trial_type,CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type,CAST(0 AS NUMERIC(10,2)) AS activation_value,FALSE AS is_not_retained FROM (SELECT user_id,context_ip,anonymous_id,order_id,received_at AS event_received_at,bundle_plan,CAST('standard' AS VARCHAR(20)) AS trial_type,ROW_NUMBER() OVER (PARTITION BY user_id,order_id ORDER BY CASE WHEN bundle_plan IS NOT NULL THEN 0 ELSE 1 END,received_at ASC) AS dedup_rank FROM (SELECT user_id,context_ip,anonymous_id,received_at,order_id,bundle_plan FROM javaScript_upentertainment_checkout.order_completed WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT user_id,context_ip,anonymous_id,received_at,order_id,bundle_plan FROM javaScript_upentertainment_checkout.order_updated WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' AND bundle_plan IS NOT NULL) le) r WHERE r.dedup_rank=1 UNION ALL SELECT user_id,context_ip,anonymous_id,id,received_at,'reacquisition','reacquisition','reacquisition',CAST(0 AS NUMERIC(10,2)),FALSE FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily') c JOIN (SELECT context_ip,anonymous_id,received_at,COALESCE(context_campaign_name,'organic') AS campaign_name,COALESCE(context_campaign_medium,'organic') AS campaign_medium,context_campaign_content AS campaign_content FROM javascript_upff_home.pages WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE) mt ON (mt.anonymous_id=c.anonymous_id OR mt.context_ip=c.context_ip) AND mt.received_at<=c.conversion_event_at AND mt.received_at>=c.conversion_event_at-(90||' days')::INTERVAL) at2) sa4
      GROUP BY order_id,user_id,conversion_event_at,conversion_event_type,trial_type,lifecycle_event_type,activation_value,is_not_retained,attributed_campaign_name,attributed_campaign_medium,attributed_campaign_content
      ) sa4
      GROUP BY 1,2,3,4
      ) m2
      JOIN (SELECT report_date,MAX(standard_trials) AS max_standard,MAX(bundle_trials) AS max_bundle,MAX(reacquisitions) AS max_reacq,MAX(activations) AS max_activations FROM (SELECT DATE(sa5.conversion_event_at) AS report_date,SUM(CASE WHEN sa5.conversion_event_type='free_trial' AND sa5.trial_type='standard' AND sa5.is_last_touch THEN 1 ELSE 0 END) AS standard_trials,SUM(CASE WHEN sa5.conversion_event_type='free_trial' AND sa5.trial_type='bundle' AND sa5.is_last_touch THEN 1 ELSE 0 END) AS bundle_trials,SUM(CASE WHEN sa5.conversion_event_type='reacquisition' AND sa5.is_last_touch THEN 1 ELSE 0 END) AS reacquisitions,COUNT(DISTINCT CASE WHEN sa5.conversion_event_type='free_trial' AND sa5.lifecycle_event_type='activation' AND sa5.is_last_touch THEN sa5.user_id END) AS activations FROM (SELECT order_id,user_id,conversion_event_at,conversion_event_type,trial_type,lifecycle_event_type,BOOL_OR(last_touch_rank=1) AS is_last_touch FROM (SELECT c.order_id,c.user_id,c.conversion_event_at,c.conversion_event_type,c.trial_type,c.lifecycle_event_type,ROW_NUMBER() OVER (PARTITION BY c.conversion_event_type,c.order_id ORDER BY mt.received_at DESC) AS last_touch_rank FROM (SELECT r.user_id,r.context_ip,r.anonymous_id,r.order_id,r.event_received_at AS conversion_event_at,'free_trial' AS conversion_event_type,r.trial_type,CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type FROM (SELECT user_id,context_ip,anonymous_id,order_id,received_at AS event_received_at,bundle_plan,CAST('standard' AS VARCHAR(20)) AS trial_type,ROW_NUMBER() OVER (PARTITION BY user_id,order_id ORDER BY CASE WHEN bundle_plan IS NOT NULL THEN 0 ELSE 1 END,received_at ASC) AS dedup_rank FROM (SELECT user_id,context_ip,anonymous_id,received_at,order_id,bundle_plan FROM javaScript_upentertainment_checkout.order_completed WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' UNION ALL SELECT user_id,context_ip,anonymous_id,received_at,order_id,bundle_plan FROM javaScript_upentertainment_checkout.order_updated WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily' AND bundle_plan IS NOT NULL) le2) r WHERE r.dedup_rank=1 UNION ALL SELECT user_id,context_ip,anonymous_id,id,received_at,'reacquisition','reacquisition','reacquisition' FROM javascript_upentertainment_checkout.order_resubscribed WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND user_id IS NOT NULL AND brand='upfaithandfamily') c JOIN (SELECT context_ip,anonymous_id,received_at FROM javascript_upff_home.pages WHERE received_at>=(CURRENT_DATE-INTERVAL '90 days')::DATE) mt ON (mt.anonymous_id=c.anonymous_id OR mt.context_ip=c.context_ip) AND mt.received_at<=c.conversion_event_at AND mt.received_at>=c.conversion_event_at-(90||' days')::INTERVAL) sa5 GROUP BY order_id,user_id,conversion_event_at,conversion_event_type,trial_type,lifecycle_event_type) sa5 GROUP BY 1,2,3,4) dn GROUP BY report_date) n2 ON m2.report_date=n2.report_date
      ) dq
      ON DATE(sa.conversion_event_at) = dq.report_date
      AND COALESCE(sa.attributed_campaign_name,    '') = COALESCE(dq.campaign_name,    '')
      AND COALESCE(sa.attributed_campaign_medium,  '') = COALESCE(dq.campaign_medium,  '')
      AND COALESCE(sa.attributed_campaign_content, '') = COALESCE(dq.campaign_content, '')

      UNION ALL

      SELECT CAST('app_trial' AS VARCHAR(20)) AS event_type, CAST(COALESCE(anonymous_id,'no_aid')||'|'||branch_row_num::TEXT AS VARCHAR(255)) AS event_id, CAST(NULL AS VARCHAR(255)) AS user_id, CAST(NULL AS VARCHAR(255)) AS context_ip, CAST(NULL AS VARCHAR(255)) AS anonymous_id, report_date::TIMESTAMP AS event_at, report_date, CAST(campaign_source AS VARCHAR(255)) AS campaign_source, CAST(campaign_name AS VARCHAR(255)) AS campaign_name, CAST(campaign_id AS VARCHAR(255)) AS campaign_id, CAST('paid' AS VARCHAR(255)) AS campaign_medium, CAST(NULL AS VARCHAR(255)) AS campaign_content, CAST(NULL AS VARCHAR(255)) AS order_id, 'free_trial' AS conversion_event_type, CAST('app' AS VARCHAR(20)) AS trial_type, CAST(NULL AS VARCHAR(255)) AS bundle_plan, FALSE AS is_bundle_user, CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type, CAST(NULL AS DATE) AS lifecycle_event_date, FALSE AS is_activated_or_reacquired, FALSE AS is_not_retained, CAST(0 AS NUMERIC(10,2)) AS activation_value, FALSE AS is_yearly_plan, CAST(NULL AS VARCHAR(20)) AS plan_type, 0 AS total_touches, CAST(NULL AS INTEGER) AS min_days_before_conversion, CAST(NULL AS TIMESTAMP) AS attributed_touch_at, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_last_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_last, CAST(0.0 AS NUMERIC(7,6)) AS credit_position_based, CAST(NULL AS VARCHAR(20)) AS touch_position, FALSE AS is_first_touch, FALSE AS is_last_touch, FALSE AS is_first_and_last, app_trial_count, 0 AS app_install_count, 0 AS app_reinstall_count, device_os, CAST(branch_channel AS VARCHAR(255)) AS branch_channel, CAST(branch_feature AS VARCHAR(100)) AS branch_feature, CAST(creative_name AS VARCHAR(500)) AS creative_name, is_paid_branch, CAST(NULL AS NUMERIC(10,2)) AS quality_score, CAST(NULL AS NUMERIC(10,2)) AS volume_score, CAST(NULL AS NUMERIC(5,4)) AS trial_to_paid_rate, CAST(NULL AS NUMERIC(5,4)) AS churn_rate, CAST(NULL AS NUMERIC(5,4)) AS retention_rate, CAST(NULL AS NUMERIC(12,2)) AS campaign_total_aov, CAST(NULL AS NUMERIC(10,2)) AS campaign_avg_aov, CAST(NULL AS NUMERIC(10,2)) AS std_trial_score, CAST(NULL AS NUMERIC(10,2)) AS bundle_trial_score, CAST(NULL AS NUMERIC(10,2)) AS reacq_score, CAST(NULL AS NUMERIC(10,2)) AS activation_score
      FROM (SELECT DATE(report_date) AS report_date, anonymous_id, os AS device_os, CASE WHEN LOWER(ad_partner)='facebook' THEN 'meta' WHEN LOWER(ad_partner) IN ('google adwords','google ads','google') THEN 'google' WHEN LOWER(ad_partner) LIKE '%tiktok%' THEN 'tiktok' WHEN LOWER(ad_partner) LIKE '%apple%search%' THEN 'apple_search' WHEN LOWER(ad_partner) LIKE '%snapchat%' THEN 'snapchat' WHEN LOWER(ad_partner) LIKE '%roku%' THEN 'roku' WHEN LOWER(ad_partner)='unattributed' THEN 'unattributed' ELSE LOWER(ad_partner) END AS campaign_source, channel AS branch_channel, campaign AS campaign_name, campaign_id, feature AS branch_feature, creative_name, count AS app_trial_count, CASE WHEN LOWER(feature)='paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch, ROW_NUMBER() OVER (ORDER BY report_date,anonymous_id,campaign,os,creative_name) AS branch_row_num FROM php.branch_purchase WHERE report_date>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND report_date<CURRENT_DATE+INTERVAL '1 day' AND count>0) branch_app_trials

      UNION ALL

      SELECT CAST('app_install' AS VARCHAR(20)) AS event_type, CAST(COALESCE(anonymous_id,'no_aid')||'|'||branch_row_num::TEXT AS VARCHAR(255)) AS event_id, CAST(NULL AS VARCHAR(255)) AS user_id, CAST(NULL AS VARCHAR(255)) AS context_ip, CAST(NULL AS VARCHAR(255)) AS anonymous_id, report_date::TIMESTAMP AS event_at, report_date, CAST(campaign_source AS VARCHAR(255)) AS campaign_source, CAST(campaign_name AS VARCHAR(255)) AS campaign_name, CAST(campaign_id AS VARCHAR(255)) AS campaign_id, CAST('paid' AS VARCHAR(255)) AS campaign_medium, CAST(NULL AS VARCHAR(255)) AS campaign_content, CAST(NULL AS VARCHAR(255)) AS order_id, CAST(NULL AS VARCHAR(20)) AS conversion_event_type, CAST('app_install' AS VARCHAR(20)) AS trial_type, CAST(NULL AS VARCHAR(255)) AS bundle_plan, FALSE AS is_bundle_user, CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type, CAST(NULL AS DATE) AS lifecycle_event_date, FALSE AS is_activated_or_reacquired, FALSE AS is_not_retained, CAST(0 AS NUMERIC(10,2)) AS activation_value, FALSE AS is_yearly_plan, CAST(NULL AS VARCHAR(20)) AS plan_type, 0 AS total_touches, CAST(NULL AS INTEGER) AS min_days_before_conversion, CAST(NULL AS TIMESTAMP) AS attributed_touch_at, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_last_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_last, CAST(0.0 AS NUMERIC(7,6)) AS credit_position_based, CAST(NULL AS VARCHAR(20)) AS touch_position, FALSE AS is_first_touch, FALSE AS is_last_touch, FALSE AS is_first_and_last, 0 AS app_trial_count, app_install_count, 0 AS app_reinstall_count, device_os, CAST(branch_channel AS VARCHAR(255)) AS branch_channel, CAST(branch_feature AS VARCHAR(100)) AS branch_feature, CAST(creative_name AS VARCHAR(500)) AS creative_name, is_paid_branch, CAST(NULL AS NUMERIC(10,2)) AS quality_score, CAST(NULL AS NUMERIC(10,2)) AS volume_score, CAST(NULL AS NUMERIC(5,4)) AS trial_to_paid_rate, CAST(NULL AS NUMERIC(5,4)) AS churn_rate, CAST(NULL AS NUMERIC(5,4)) AS retention_rate, CAST(NULL AS NUMERIC(12,2)) AS campaign_total_aov, CAST(NULL AS NUMERIC(10,2)) AS campaign_avg_aov, CAST(NULL AS NUMERIC(10,2)) AS std_trial_score, CAST(NULL AS NUMERIC(10,2)) AS bundle_trial_score, CAST(NULL AS NUMERIC(10,2)) AS reacq_score, CAST(NULL AS NUMERIC(10,2)) AS activation_score
      FROM (SELECT DATE(report_date) AS report_date, anonymous_id, os AS device_os, CASE WHEN LOWER(ad_partner)='facebook' THEN 'meta' WHEN LOWER(ad_partner) IN ('google adwords','google ads','google') THEN 'google' WHEN LOWER(ad_partner) LIKE '%tiktok%' THEN 'tiktok' WHEN LOWER(ad_partner) LIKE '%apple%search%' THEN 'apple_search' WHEN LOWER(ad_partner) LIKE '%snapchat%' THEN 'snapchat' WHEN LOWER(ad_partner) LIKE '%roku%' THEN 'roku' WHEN LOWER(ad_partner)='unattributed' THEN 'unattributed' ELSE LOWER(ad_partner) END AS campaign_source, channel AS branch_channel, campaign AS campaign_name, campaign_id, feature AS branch_feature, creative_name, count AS app_install_count, CASE WHEN LOWER(feature)='paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch, ROW_NUMBER() OVER (ORDER BY report_date,anonymous_id,campaign,os,creative_name) AS branch_row_num FROM php.branch_install WHERE report_date>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND report_date<CURRENT_DATE+INTERVAL '1 day' AND count>0) branch_app_installs

      UNION ALL

      SELECT CAST('app_reinstall' AS VARCHAR(20)) AS event_type, CAST(COALESCE(anonymous_id,'no_aid')||'|'||branch_row_num::TEXT AS VARCHAR(255)) AS event_id, CAST(NULL AS VARCHAR(255)) AS user_id, CAST(NULL AS VARCHAR(255)) AS context_ip, CAST(NULL AS VARCHAR(255)) AS anonymous_id, report_date::TIMESTAMP AS event_at, report_date, CAST(campaign_source AS VARCHAR(255)) AS campaign_source, CAST(campaign_name AS VARCHAR(255)) AS campaign_name, CAST(campaign_id AS VARCHAR(255)) AS campaign_id, CAST('paid' AS VARCHAR(255)) AS campaign_medium, CAST(NULL AS VARCHAR(255)) AS campaign_content, CAST(NULL AS VARCHAR(255)) AS order_id, CAST(NULL AS VARCHAR(20)) AS conversion_event_type, CAST('app_reinstall' AS VARCHAR(20)) AS trial_type, CAST(NULL AS VARCHAR(255)) AS bundle_plan, FALSE AS is_bundle_user, CAST(NULL AS VARCHAR(20)) AS lifecycle_event_type, CAST(NULL AS DATE) AS lifecycle_event_date, FALSE AS is_activated_or_reacquired, FALSE AS is_not_retained, CAST(0 AS NUMERIC(10,2)) AS activation_value, FALSE AS is_yearly_plan, CAST(NULL AS VARCHAR(20)) AS plan_type, 0 AS total_touches, CAST(NULL AS INTEGER) AS min_days_before_conversion, CAST(NULL AS TIMESTAMP) AS attributed_touch_at, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_last_touch, CAST(0.0 AS NUMERIC(7,6)) AS credit_first_last, CAST(0.0 AS NUMERIC(7,6)) AS credit_position_based, CAST(NULL AS VARCHAR(20)) AS touch_position, FALSE AS is_first_touch, FALSE AS is_last_touch, FALSE AS is_first_and_last, 0 AS app_trial_count, 0 AS app_install_count, app_reinstall_count, device_os, CAST(branch_channel AS VARCHAR(255)) AS branch_channel, CAST(branch_feature AS VARCHAR(100)) AS branch_feature, CAST(creative_name AS VARCHAR(500)) AS creative_name, is_paid_branch, CAST(NULL AS NUMERIC(10,2)) AS quality_score, CAST(NULL AS NUMERIC(10,2)) AS volume_score, CAST(NULL AS NUMERIC(5,4)) AS trial_to_paid_rate, CAST(NULL AS NUMERIC(5,4)) AS churn_rate, CAST(NULL AS NUMERIC(5,4)) AS retention_rate, CAST(NULL AS NUMERIC(12,2)) AS campaign_total_aov, CAST(NULL AS NUMERIC(10,2)) AS campaign_avg_aov, CAST(NULL AS NUMERIC(10,2)) AS std_trial_score, CAST(NULL AS NUMERIC(10,2)) AS bundle_trial_score, CAST(NULL AS NUMERIC(10,2)) AS reacq_score, CAST(NULL AS NUMERIC(10,2)) AS activation_score
      FROM (SELECT DATE(report_date) AS report_date, anonymous_id, os AS device_os, CASE WHEN LOWER(ad_partner)='facebook' THEN 'meta' WHEN LOWER(ad_partner) IN ('google adwords','google ads','google') THEN 'google' WHEN LOWER(ad_partner) LIKE '%tiktok%' THEN 'tiktok' WHEN LOWER(ad_partner) LIKE '%apple%search%' THEN 'apple_search' WHEN LOWER(ad_partner) LIKE '%snapchat%' THEN 'snapchat' WHEN LOWER(ad_partner) LIKE '%roku%' THEN 'roku' WHEN LOWER(ad_partner)='unattributed' THEN 'unattributed' ELSE LOWER(ad_partner) END AS campaign_source, channel AS branch_channel, campaign AS campaign_name, campaign_id, feature AS branch_feature, creative_name, count AS app_reinstall_count, CASE WHEN LOWER(feature)='paid advertising' THEN TRUE ELSE FALSE END AS is_paid_branch, ROW_NUMBER() OVER (ORDER BY report_date,anonymous_id,campaign,os,creative_name) AS branch_row_num FROM php.branch_reinstall WHERE report_date>=(CURRENT_DATE-INTERVAL '90 days')::DATE AND report_date<CURRENT_DATE+INTERVAL '1 day' AND count>0) branch_app_reinstalls

      ) all_rows
      WHERE (
      {% incrementcondition %} report_date {% endincrementcondition %}
      )
      ;;
  }

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

  dimension: event_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.event_id || '|' || ${TABLE}.event_type || '|' || COALESCE(${TABLE}.touch_position,'na') || '|' || COALESCE(${TABLE}.campaign_name,'no_campaign') || '|' || COALESCE(${TABLE}.report_date::TEXT,'no_date') || '|' || COALESCE(${TABLE}.device_os,'no_os') || '|' || COALESCE(${TABLE}.creative_name,'no_creative') ;;
  }

  dimension: event_type { type: string description: "'page_visit' | 'conversion' | 'app_trial' | 'app_install' | 'app_reinstall'" sql: ${TABLE}.event_type ;; }
  dimension: user_id      { type: string sql: ${TABLE}.user_id ;; }
  dimension: anonymous_id { type: string sql: ${TABLE}.anonymous_id ;; }
  dimension: context_ip   { type: string sql: ${TABLE}.context_ip ;; }

  dimension_group: event        { type: time timeframes: [raw, time, date, week, month, quarter, year] sql: ${TABLE}.event_at ;; }
  dimension_group: report_date  { type: time label: "Report Date" timeframes: [date, week, month, quarter, year] convert_tz: no datatype: date sql: ${TABLE}.report_date ;; }
  dimension_group: attributed_touch { type: time timeframes: [raw, time, date] sql: ${TABLE}.attributed_touch_at ;; }
  dimension: lifecycle_event_date { type: date convert_tz: no datatype: date sql: ${TABLE}.lifecycle_event_date ;; }

  dimension: campaign_source  { type: string label: "Campaign Source"  sql: ${TABLE}.campaign_source ;; }
  dimension: campaign_name    { type: string label: "Campaign Name"    sql: ${TABLE}.campaign_name ;; }
  dimension: campaign_id      { type: string label: "Campaign ID"      sql: ${TABLE}.campaign_id ;; }
  dimension: campaign_medium  { type: string label: "Campaign Medium"  sql: ${TABLE}.campaign_medium ;; }
  dimension: campaign_content { type: string label: "Campaign Content" sql: ${TABLE}.campaign_content ;; }

  dimension: marketing_platform {
    type: string
    label: "Marketing Platform"
    sql:
      CASE
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords') AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g') AND LOWER(${TABLE}.campaign_name) LIKE '%display%' THEN 'Google Display'
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords') AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g') AND (LOWER(${TABLE}.campaign_name) LIKE '%pmax%' OR LOWER(${TABLE}.campaign_name) LIKE '%performance max%') THEN 'Google PMax'
        WHEN LOWER(${TABLE}.campaign_source) IN ('google','google_ads','adwords') AND LOWER(${TABLE}.campaign_medium) IN ('cpc','ppc','paid','g') THEN 'Google Search'
        WHEN LOWER(${TABLE}.campaign_source) IN ('meta','instagram','ig','fb','an','campaign.name') OR LOWER(${TABLE}.campaign_source) LIKE 'meta%' THEN 'Meta Ads'
        WHEN LOWER(${TABLE}.campaign_source) IN ('bing','microsoft','msn') THEN 'Bing Ads'
        WHEN LOWER(${TABLE}.campaign_source) IN ('hubspot','hubspot_upff','hubspot_uptv') OR LOWER(${TABLE}.campaign_medium) LIKE 'email%' THEN 'HubSpot'
        WHEN LOWER(${TABLE}.campaign_source) LIKE '%uptv%' THEN 'UPtv Digital'
        WHEN LOWER(${TABLE}.campaign_medium) = 'organic' AND LOWER(${TABLE}.campaign_source) IN ('google','bing','duckduckgo','yahoo') THEN 'Organic Search'
        WHEN LOWER(${TABLE}.campaign_medium) IN ('social','organic_social') OR (LOWER(${TABLE}.campaign_medium)='organic' AND LOWER(${TABLE}.campaign_source) IN ('facebook','instagram','tiktok','x','twitter','linkedin')) THEN 'Organic Social'
        WHEN ${TABLE}.campaign_source = 'organic' THEN 'Others'
        WHEN ${TABLE}.campaign_source = 'direct'  THEN 'Others'
        WHEN ${TABLE}.campaign_source IS NULL      THEN 'Unknown'
        ELSE 'Others'
      END ;;
  }

  dimension: device_os      { type: string label: "Device OS"        sql: ${TABLE}.device_os ;; }
  dimension: branch_channel { type: string label: "Branch Channel"   sql: ${TABLE}.branch_channel ;; }
  dimension: branch_feature { type: string label: "Branch Feature"   sql: ${TABLE}.branch_feature ;; }
  dimension: creative_name  { type: string label: "Creative Name"    sql: ${TABLE}.creative_name ;; }
  dimension: is_paid_branch { type: yesno  label: "Is Paid (Branch)" sql: ${TABLE}.is_paid_branch ;; }
  dimension: surface { type: string label: "Surface" sql: CASE WHEN ${TABLE}.event_type IN ('app_trial','app_install','app_reinstall') THEN 'app' ELSE 'web' END ;; }

  dimension: order_id              { type: string sql: ${TABLE}.order_id ;; }
  dimension: conversion_event_type { type: string sql: ${TABLE}.conversion_event_type ;; }
  dimension: trial_type            { type: string sql: ${TABLE}.trial_type ;; }
  dimension: bundle_plan           { type: string sql: ${TABLE}.bundle_plan ;; }
  dimension: is_bundle_user        { type: yesno  sql: ${TABLE}.is_bundle_user ;; }
  dimension: lifecycle_event_type       { type: string sql: ${TABLE}.lifecycle_event_type ;; }
  dimension: is_activated_or_reacquired { type: yesno  sql: ${TABLE}.is_activated_or_reacquired ;; }
  dimension: is_activated               { type: yesno  sql: ${TABLE}.lifecycle_event_type = 'activation' ;; }
  dimension: is_not_retained            { type: yesno  sql: ${TABLE}.is_not_retained ;; }
  dimension: total_touches              { type: number sql: ${TABLE}.total_touches ;; }
  dimension: days_before_conversion     { type: number sql: ${TABLE}.min_days_before_conversion ;; hidden: yes }

  dimension: within_attribution_window {
    type: yesno
    hidden: yes
    sql: ${TABLE}.event_type IN ('page_visit','app_trial','app_install','app_reinstall') OR ${TABLE}.min_days_before_conversion IS NULL OR ${TABLE}.min_days_before_conversion <= {% parameter attribution_window_days %} ;;
  }

  dimension: activation_value { type: number label: "Activation Value" sql: ${TABLE}.activation_value ;; value_format_name: usd }
  dimension: is_yearly_plan   { type: yesno  sql: ${TABLE}.is_yearly_plan ;; }
  dimension: plan_type        { type: string sql: ${TABLE}.plan_type ;; }

  dimension: credit_weight {
    type: number
    value_format_name: decimal_4
    sql:
      CASE '{% parameter attribution_model %}'
        WHEN 'first_touch'    THEN ${TABLE}.credit_first_touch
        WHEN 'first_last'     THEN ${TABLE}.credit_first_last
        WHEN 'position_based' THEN ${TABLE}.credit_position_based
        ELSE                       ${TABLE}.credit_last_touch
      END ;;
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
      END ;;
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
      END ;;
  }

  dimension: selected_attribution_window_days { type: number label: "Selected Attribution Window (Days)" sql: {% parameter attribution_window_days %} ;; }
  dimension: quality_score  { type: number label: "Quality Score" sql: ${TABLE}.quality_score ;; value_format_name: decimal_2 }
  dimension: volume_score   { type: number sql: ${TABLE}.volume_score ;; value_format_name: decimal_2 }

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
      END ;;
  }

  dimension: quality_tier {
    type: string
    sql:
      CASE
        WHEN ${TABLE}.quality_score >= 60 THEN '1 — Top'
        WHEN ${TABLE}.quality_score >= 30 THEN '2 — Mid'
        WHEN ${TABLE}.quality_score IS NOT NULL THEN '3 — Low'
        ELSE NULL
      END ;;
  }

  dimension: trial_to_paid_rate { type: number label: "Trial → Paid Rate (Daily)" sql: ${TABLE}.trial_to_paid_rate ;; value_format_name: percent_2 }
  dimension: churn_rate         { type: number label: "Churn Rate (Daily)"        sql: ${TABLE}.churn_rate ;;        value_format_name: percent_2 }
  dimension: retention_rate     { type: number label: "Retention Rate (Daily)"    sql: ${TABLE}.retention_rate ;;    value_format_name: percent_2 }
  dimension: campaign_total_aov { type: number label: "Campaign Total Order Value (Daily)" sql: ${TABLE}.campaign_total_aov ;; value_format_name: usd_0 }
  dimension: campaign_avg_aov   { type: number label: "Campaign Avg Order Value (Daily)"   sql: ${TABLE}.campaign_avg_aov ;;   value_format_name: usd }
  dimension: std_trial_score    { type: number hidden: yes sql: ${TABLE}.std_trial_score ;; }
  dimension: bundle_trial_score { type: number hidden: yes sql: ${TABLE}.bundle_trial_score ;; }
  dimension: reacq_score        { type: number hidden: yes sql: ${TABLE}.reacq_score ;; }
  dimension: activation_score   { type: number hidden: yes sql: ${TABLE}.activation_score ;; }

  measure: distinct_web_visits   { type: count_distinct label: "Distinct Web Visits"         sql: COALESCE(${TABLE}.user_id,${TABLE}.anonymous_id) ;; filters: [event_type: "page_visit"] drill_fields: [drill_visits*] }
  measure: total_visits          { type: count          label: "Total Visits"                filters: [event_type: "page_visit"] }
  measure: web_trials_started    { type: count          label: "Free Trials Started (Web)"   filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"] drill_fields: [drill_conversions*] }
  measure: free_trials_started   { type: count          label: "Free Trials Started"         filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"] drill_fields: [drill_conversions*] }
  measure: free_trials_converted { type: count_distinct label: "Free Trials Converted"       sql: ${TABLE}.user_id ;; filters: [event_type: "conversion", conversion_event_type: "free_trial", is_activated: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"] drill_fields: [drill_conversions*] }
  measure: reacquisitions        { type: count          label: "Reacquisitions"              filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"] drill_fields: [drill_conversions*] }
  measure: trial_to_paid_conversion_rate { type: number label: "Trial to Paid Conversion Rate" sql: 1.0*${free_trials_converted}/NULLIF(${web_trials_started},0) ;; value_format_name: percent_2 }
  measure: visit_to_trial_rate   { type: number label: "Visit → Trial Rate" sql: 1.0*${web_trials_started}/NULLIF(${distinct_web_visits},0) ;; value_format_name: percent_2 }

  measure: app_trials_started      { type: sum label: "Free Trials Started (App)"            sql: ${TABLE}.app_trial_count ;;   filters: [event_type: "app_trial"] }
  measure: app_trials_started_paid { type: sum label: "Free Trials Started (App, Paid Only)" sql: ${TABLE}.app_trial_count ;;   filters: [event_type: "app_trial", is_paid_branch: "yes"] }
  measure: app_installs            { type: sum label: "App Installs"                          sql: ${TABLE}.app_install_count ;; filters: [event_type: "app_install"] }
  measure: app_installs_paid       { type: sum label: "App Installs (Paid Only)"              sql: ${TABLE}.app_install_count ;; filters: [event_type: "app_install", is_paid_branch: "yes"] }
  measure: app_reinstalls          { type: sum label: "App Reinstalls"                        sql: ${TABLE}.app_reinstall_count ;; filters: [event_type: "app_reinstall"] }
  measure: app_reinstalls_paid     { type: sum label: "App Reinstalls (Paid Only)"            sql: ${TABLE}.app_reinstall_count ;; filters: [event_type: "app_reinstall", is_paid_branch: "yes"] }
  measure: app_installs_and_reinstalls       { type: number label: "App Installs + Reinstalls"          sql: ${app_installs}+${app_reinstalls} ;; }
  measure: total_app_installs_paid           { type: number label: "Total App Re/Installs (Paid Only)"  sql: ${app_installs_paid}+${app_reinstalls_paid} ;; }
  measure: app_reinstall_to_trial_rate       { type: number label: "App Reinstall → Trial Rate"         sql: 1.0*${app_trials_started}/NULLIF(${app_reinstalls},0) ;; value_format_name: percent_2 }
  measure: total_trials_started              { type: number label: "Free Trials Started (Web + App(Paid))" sql: ${web_trials_started}+${app_trials_started_paid} ;; }
  measure: app_share_of_trials               { type: number label: "% App Trials"                       sql: 1.0*${app_trials_started}/NULLIF(${web_trials_started}+${app_trials_started},0) ;; value_format_name: percent_2 }
  measure: app_install_to_trial_rate         { type: number label: "App Install → Trial Rate"           sql: 1.0*${app_trials_started}/NULLIF(${app_installs},0) ;; value_format_name: percent_2 }
  measure: app_install_to_trial_rate_paid    { type: number label: "App Install → Trial Rate (Paid Only)" sql: 1.0*${app_trials_started_paid}/NULLIF(${app_installs_paid},0) ;; value_format_name: percent_2 }
  measure: total_app_re_installs_to_trial_rate_paid { type: number label: "Total App Re/Installs → Trial Rate (Paid Only)" sql: 1.0*${app_trials_started_paid}/NULLIF((${app_installs_paid}+${app_reinstalls_paid}),0) ;; value_format_name: percent_2 }

  measure: avg_touches_per_conversion    { type: average label: "Avg Touches per Conversion"    sql: ${TABLE}.total_touches ;; filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"] value_format_name: decimal_2 }
  measure: avg_touches_per_trial         { type: average label: "Avg Touches per Free Trial"    sql: ${TABLE}.total_touches ;; filters: [event_type: "conversion", conversion_event_type: "free_trial", is_primary_attribution: "yes", within_attribution_window: "yes"] value_format_name: decimal_2 }
  measure: avg_touches_per_reacquisition { type: average label: "Avg Touches per Reacquisition" sql: ${TABLE}.total_touches ;; filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"] value_format_name: decimal_2 }
  measure: max_touches_per_conversion    { type: max     label: "Max Touches in a Journey"      sql: ${TABLE}.total_touches ;; filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: median_touches_per_conversion { type: median  label: "Median Touches per Conversion" sql: ${TABLE}.total_touches ;; filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"] value_format_name: decimal_1 }

  measure: not_retained_users { type: count_distinct label: "Not Retained Users" sql: ${TABLE}.user_id ;; filters: [event_type: "conversion", conversion_event_type: "free_trial", is_not_retained: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: avg_retention_rate { type: average label: "Avg Retention Rate" sql: ${TABLE}.retention_rate ;; filters: [event_type: "conversion", is_primary_attribution: "yes"] value_format_name: percent_2 }
  measure: avg_churn_rate     { type: average label: "Avg Churn Rate"     sql: ${TABLE}.churn_rate ;;     filters: [event_type: "conversion", is_primary_attribution: "yes"] value_format_name: percent_2 }

  measure: avg_order_value    { type: average      label: "Avg Order Value"   sql: ${TABLE}.activation_value ;; filters: [event_type: "conversion", is_primary_attribution: "yes", activation_value: ">0", within_attribution_window: "yes"] value_format_name: usd }
  measure: total_order_value  { type: sum          label: "Total Order Value"  sql: ${TABLE}.activation_value ;; filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"] value_format_name: usd_0 }
  measure: order_value_per_trial { type: number    label: "Order Value per Trial" sql: 1.0*${total_order_value}/NULLIF(${web_trials_started},0) ;; value_format_name: usd }
  measure: yearly_plan_users  { type: count_distinct label: "Yearly Plan Users"  sql: ${TABLE}.user_id ;; filters: [event_type: "conversion", is_primary_attribution: "yes", is_yearly_plan: "yes", within_attribution_window: "yes"] }
  measure: monthly_plan_users { type: count_distinct label: "Monthly Plan Users" sql: ${TABLE}.user_id ;; filters: [event_type: "conversion", is_primary_attribution: "yes", is_yearly_plan: "no", activation_value: ">0", within_attribution_window: "yes"] }
  measure: pct_yearly_plan  { type: number label: "% Yearly Plan Subs"  sql: 1.0*${yearly_plan_users}/NULLIF(${yearly_plan_users}+${monthly_plan_users},0) ;; value_format_name: percent_2 }
  measure: pct_monthly_plan { type: number label: "% Monthly Plan Subs" sql: 1.0*${monthly_plan_users}/NULLIF(${yearly_plan_users}+${monthly_plan_users},0) ;; value_format_name: percent_2 }

  measure: avg_quality_score { type: average      label: "Avg Quality Score"      sql: ${TABLE}.quality_score ;; filters: [event_type: "conversion", is_primary_attribution: "yes"] value_format_name: decimal_2 drill_fields: [drill_quality*] }
  measure: max_quality_score { type: max          label: "Best Day Quality Score" sql: ${TABLE}.quality_score ;; filters: [event_type: "conversion", is_primary_attribution: "yes"] value_format_name: decimal_2 }
  measure: top_grade_days    { type: count_distinct label: "Days Graded A or B"  sql: ${TABLE}.report_date ;; filters: [event_type: "conversion", is_primary_attribution: "yes", quality_grade: "A,B"] }

  measure: total_converted     { type: count_distinct label: "Total Converted"    sql: ${TABLE}.user_id ;; filters: [event_type: "conversion", conversion_event_type: "free_trial", is_activated: "yes", is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: total_reacquisition { type: count         label: "Total Reacquisition" filters: [event_type: "conversion", conversion_event_type: "reacquisition", is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: standard_trials     { type: count         label: "Standard Trials"     filters: [event_type: "conversion", conversion_event_type: "free_trial", trial_type: "standard", is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: bundle_trials       { type: count         label: "Bundle Trials"       filters: [event_type: "conversion", conversion_event_type: "free_trial", trial_type: "bundle",   is_primary_attribution: "yes", within_attribution_window: "yes"] }
  measure: total_conversions   { type: count         label: "Total Conversions"   filters: [event_type: "conversion", is_primary_attribution: "yes", within_attribution_window: "yes"] }

  measure: free_trials_started_weighted { type: sum label: "Free Trials Started (Credit-Weighted)"  sql: ${credit_weight} ;; filters: [event_type: "conversion", conversion_event_type: "free_trial",    within_attribution_window: "yes"] value_format_name: decimal_4 }
  measure: reacquisitions_weighted      { type: sum label: "Reacquisitions (Credit-Weighted)"        sql: ${credit_weight} ;; filters: [event_type: "conversion", conversion_event_type: "reacquisition", within_attribution_window: "yes"] value_format_name: decimal_4 }
  measure: first_touch_credit  { type: sum label: "First Touch Credit"  sql: ${credit_weight} ;; filters: [event_type: "conversion", touch_position: "first",  within_attribution_window: "yes"] value_format_name: decimal_4 }
  measure: middle_touch_credit { type: sum label: "Middle Touch Credit" sql: ${credit_weight} ;; filters: [event_type: "conversion", touch_position: "middle", within_attribution_window: "yes"] value_format_name: decimal_4 }
  measure: last_touch_credit   { type: sum label: "Last Touch Credit"   sql: ${credit_weight} ;; filters: [event_type: "conversion", touch_position: "last",   within_attribution_window: "yes"] value_format_name: decimal_4 }

  set: drill_visits      { fields: [report_date_date, marketing_platform, campaign_source, campaign_medium, campaign_name, total_visits, distinct_web_visits] }
  set: drill_conversions { fields: [report_date_date, user_id, order_id, conversion_event_type, trial_type, plan_type, activation_value, surface, device_os, marketing_platform, campaign_source, campaign_medium, campaign_name, campaign_content, lifecycle_event_type, lifecycle_event_date, is_not_retained, total_touches, touch_position, credit_weight, quality_score, quality_grade] }
  set: drill_quality     { fields: [report_date_date, campaign_name, campaign_medium, campaign_content, marketing_platform, surface, standard_trials, bundle_trials, reacquisitions, total_converted, not_retained_users, avg_touches_per_conversion, retention_rate, trial_to_paid_rate, avg_order_value, pct_yearly_plan, pct_monthly_plan, quality_score, quality_grade] }
  set: campaign_subscription_events_set { fields: [campaign_name, campaign_content, marketing_platform, surface, device_os, campaign_medium, web_trials_started, app_trials_started, app_installs, app_reinstalls, app_installs_and_reinstalls, total_trials_started, app_share_of_trials, app_install_to_trial_rate, app_reinstall_to_trial_rate, free_trials_started_weighted, total_converted, total_reacquisition, avg_touches_per_trial, avg_order_value, pct_yearly_plan, pct_monthly_plan, avg_quality_score, quality_grade] }
}

datagroup: marketing_attribution_daily {
  sql_trigger: SELECT TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '2 hour', 'YYYY-MM-DD') ;;
  max_cache_age: "24 hours"
}
