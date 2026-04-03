view: churn {
  derived_table: {
    sql:
        WITH cfg AS (
    SELECT report_date
    FROM ${configg.SQL_TABLE_NAME}
),

v2_table AS (
    SELECT *
    FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
    WHERE report_date >= (SELECT MAX(report_date) FROM cfg)
),

-- -----------------------------
-- Base datasets (unchanged)
-- -----------------------------
non_web_user AS (
    SELECT
        report_date,
        user_id,
        billing_period,
        platform
    FROM v2_table
    WHERE platform != 'Chargebee'
),

non_web_cancelled AS (
    SELECT
        DATE("timestamp") AS report_date,
        CAST(user_id AS VARCHAR) AS user_id
    FROM vimeo_ott_webhook.customer_product_expired
    WHERE DATE("timestamp") >= (SELECT MAX(report_date) FROM cfg)
),

non_web_cancelled2 AS (
    SELECT
        a.report_date,
        a.user_id,
        b.billing_period,
        b.platform
    FROM non_web_cancelled a
    LEFT JOIN non_web_user b
        ON a.report_date = b.report_date
       AND a.user_id = b.user_id

    UNION ALL

    SELECT
        DATE(timestamp) AS report_date,
        user_id,
        subscription_frequency AS billing_period,
        platform
    FROM vimeo_ott_webhook.customer_product_disabled
    WHERE platform != 'api'
),

chargebee_cancelled AS (
    SELECT
        content_subscription_id::VARCHAR AS user_id,
        CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
        END AS billing_period,
        DATE("timestamp") AS report_date,
        'web' AS platform
    FROM chargebee_webhook_events.subscription_cancelled
    WHERE
        (content_subscription_cancelled_at - content_subscription_activated_at) > 10000
        AND content_subscription_subscription_items LIKE '%UP%'
        AND DATE(timestamp) >= (SELECT MAX(report_date) FROM cfg)

    UNION ALL

    SELECT
        content_subscription_id::VARCHAR AS user_id,
        CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
        END AS billing_period,
        DATE("timestamp") AS report_date,
        'web' AS platform
    FROM chargebee_webhook_events.subscription_paused
    WHERE content_subscription_subscription_items LIKE '%UP%'
        AND DATE(timestamp) >= (SELECT MAX(report_date) FROM cfg)
),

-- -----------------------------
-- Aggregate churn
-- -----------------------------
result AS (
    SELECT
        COUNT(DISTINCT user_id) AS user_count,
        report_date,
        billing_period,
        platform
    FROM non_web_cancelled2
    GROUP BY 2,3,4

    UNION ALL

    SELECT
        COUNT(DISTINCT user_id) AS user_count,
        report_date,
        billing_period,
        platform
    FROM chargebee_cancelled
    GROUP BY 2,3,4
),

-- -----------------------------
-- Split monthly / yearly
-- -----------------------------
monthly AS (
    SELECT *
    FROM result
    WHERE billing_period = 'monthly'
),

yearly AS (
    SELECT *
    FROM result
    WHERE billing_period = 'yearly'
)

-- -----------------------------
-- Final output
-- -----------------------------
-- monthly stays as-is
SELECT
    CASE
        WHEN billing_period = 'monthly' AND report_date BETWEEN '2026-01-10' AND '2026-01-12' THEN 64
        WHEN billing_period = 'monthly' AND report_date BETWEEN '2026-01-21' AND '2026-01-25' THEN 200
        ELSE user_count
    END AS user_count,
    report_date,
    billing_period,
    platform
FROM monthly

UNION ALL

-- yearly filled from monthly backbone
SELECT
    CASE
        WHEN m.platform = 'android' AND m.report_date BETWEEN '2026-01-10' AND '2026-01-12' THEN 5
        WHEN m.platform = 'android_tv' AND m.report_date BETWEEN '2026-01-10' AND '2026-01-12' THEN 1
        WHEN m.platform = 'roku' AND m.report_date BETWEEN '2026-01-21' AND '2026-01-25' THEN 13
        ELSE COALESCE(y.user_count, 0)
    END AS user_count,
    m.report_date,
    'yearly' AS billing_period,
    m.platform
FROM monthly m
LEFT JOIN yearly y
    ON m.report_date = y.report_date
   AND m.platform = y.platform





      ;;
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }


  dimension: report_date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension: billing_period{
    type: string
    sql: ${TABLE}.billing_period ;;
  }


  }
