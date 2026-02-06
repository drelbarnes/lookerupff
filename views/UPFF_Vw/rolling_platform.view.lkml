view: rolling_platform {
  derived_table: {
    sql:

-- 1) Base filtered table
,v2_table AS (
  SELECT *
  FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
  WHERE report_date >= '2025-06-30'
),

      -- 2) Chargebee cancellations -> treat as 'web' platform (adjust if you prefer a different label)
      user_cancelled_counts2 AS (

 SELECT
        content_subscription_id::VARCHAR AS user_id,
        CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
        END AS billing_period,
        DATE("timestamp") AS report_date,
        DATE_TRUNC('month', report_date) AS month_start,

        'web'::VARCHAR AS platform
        FROM chargebee_webhook_events.subscription_cancelled
        WHERE
        (
        --(content_subscription_cancel_reason_code not in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer') and
        (content_subscription_cancelled_at - content_subscription_activated_at) > 10000)
        --or content_subscription_cancel_reason_code is null)
        AND content_subscription_subscription_items LIKE '%UP%'
      ),

      -- 33) Non-Chargebee users with platform/billing info (to enrich VM webhook expirations)
      vm_user AS (
      SELECT
      report_date,
      user_id,
      billing_period,
      platform
      FROM v2_table
      WHERE platform != 'Chargebee'
      ),

      -- 4) Vimeo OTT webhook expirations (source of cancellations)
      vm AS (
      SELECT
      DATE(timestamp)                    AS report_date,
      CAST(user_id AS VARCHAR)           AS user_id,
      DATE_TRUNC('month', timestamp)     AS month_start
      FROM vimeo_ott_webhook.customer_product_expired
      WHERE DATE(timestamp) >= '2025-07-01'

      UNION ALL

      SELECT
      DATE(timestamp)                    AS report_date,
      CAST(user_id AS VARCHAR)           AS user_id,
      DATE_TRUNC('month', timestamp)     AS month_start
      FROM vimeo_ott_webhook.customer_product_disabled
      WHERE DATE(timestamp) >= '2025-07-01'
      ),

      -- 5) Map VM expirations to user metadata; avoid NULL platform buckets
      vm2 AS (
      SELECT
      a.report_date,
      a.user_id,
      b.billing_period,
      a.month_start,
      COALESCE(b.platform, 'unknown') AS platform
      FROM vm a
      LEFT JOIN vm_user b
      ON a.report_date = b.report_date
      AND a.user_id     = b.user_id
      ),

      -- 6) Union all cancellations across sources (now with platform)
      user_cancelled_counts AS (
      SELECT report_date, user_id, billing_period, month_start, platform
      FROM user_cancelled_counts2
      UNION ALL
      SELECT report_date, user_id, billing_period, month_start, platform
      FROM vm2
      ),

      -- 7) 30-day rolling unique cancellations by platform
      rolling_churn AS (
      SELECT
      t1.report_date,
      t1.platform,
      COUNT(DISTINCT CASE WHEN t2.billing_period = 'monthly' THEN t2.user_id END) AS rolling_30_day_unique_user_count_monthly,
      COUNT(DISTINCT CASE WHEN t2.billing_period = 'yearly'  THEN t2.user_id END) AS rolling_30_day_unique_user_count_yearly
      FROM user_cancelled_counts t1
      JOIN user_cancelled_counts t2
      ON t2.report_date BETWEEN t1.report_date - INTERVAL '29 days' AND t1.report_date
      AND t2.platform = t1.platform       -- important: scope rolling window to same platform
      GROUP BY t1.report_date, t1.platform
      ),

      -- 8) iOS paid subs (source of truth for iOS only)
      new_apple0 AS (
      SELECT *
      FROM ${ios.SQL_TABLE_NAME}
      ),

      -- 9) Pivot iOS paid subs into monthly/yearly columns
      new_apple2 AS (
      SELECT
      a.report_date,
      a.paid_subscribers AS total_paid_subs_monthly,
      b.paid_subscribers AS total_paid_subs_yearly
      FROM (SELECT * FROM new_apple0 WHERE billing_period = 'monthly') a
      LEFT JOIN (SELECT * FROM new_apple0 WHERE billing_period = 'yearly') b
      ON a.report_date = b.report_date
      ),

      -- 10) Paid subs by platform for non-iOS, then union iOS
      total_paid_subs AS (
      SELECT
      report_date,
      CASE
      WHEN platform = 'Chargebee' THEN 'web'
      ELSE platform
      END AS platform,
      COUNT(DISTINCT CASE
      WHEN (status LIKE 'non_renewing' OR status IN ('active','enabled'))
      AND billing_period = 'monthly' THEN user_id END
      ) AS total_paid_subs_monthly,
      COUNT(DISTINCT CASE
      WHEN (status LIKE 'non_renewing' OR status IN ('active','enabled'))
      AND billing_period = 'yearly'  THEN user_id END
      ) AS total_paid_subs_yearly
      FROM v2_table
      WHERE platform != 'ios'
      GROUP BY report_date, platform
      ),

      total_paid_subs1 AS (
      SELECT
      t.report_date::date  AS report_date,
      t.platform::varchar  AS platform,
      t.total_paid_subs_monthly::bigint AS total_paid_subs_monthly,
      t.total_paid_subs_yearly::bigint  AS total_paid_subs_yearly
      FROM total_paid_subs t
      UNION ALL
      SELECT
      na.report_date::date,
      CAST('ios' AS varchar)           AS platform,
      na.total_paid_subs_monthly::bigint,
      na.total_paid_subs_yearly::bigint
      FROM new_apple2 na
      ),

      -- 11) Collapse to one row per date × platform (if duplicates exist)
      total_paid_subs2 AS (
      SELECT
      report_date,
      platform,
      SUM(total_paid_subs_monthly) AS total_paid_subs_monthly,
      SUM(total_paid_subs_yearly)  AS total_paid_subs_yearly
      FROM total_paid_subs1
      GROUP BY report_date, platform
      ),

      -- 12) Final join + platform-partitioned LAGs
      result AS (
      SELECT
      rc.report_date,
      rc.platform,
      rc.rolling_30_day_unique_user_count_yearly,
      rc.rolling_30_day_unique_user_count_monthly,
      tps.total_paid_subs_yearly,
      tps.total_paid_subs_monthly,
      LAG(tps.total_paid_subs_monthly, 30) OVER (
      PARTITION BY tps.platform ORDER BY tps.report_date
      ) AS total_rolling_monthly,
      LAG(tps.total_paid_subs_yearly, 30) OVER (
      PARTITION BY tps.platform ORDER BY tps.report_date
      ) AS total_rolling_yearly
      FROM rolling_churn rc
      LEFT JOIN total_paid_subs2 tps
      ON rc.report_date = tps.report_date
      AND rc.platform    = tps.platform
      )

      SELECT *
      FROM result
      ORDER BY report_date, platform;;

    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -555, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]
  }

  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
    primary_key: yes
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;

  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }


  dimension: total_paid_subs_yearly {
    type: number
    sql: ${TABLE}.total_paid_subs_yearly ;;
  }

  dimension: total_paid_subs_monthly {
    type: number
    sql: ${TABLE}.total_paid_subs_monthly ;;
  }


  dimension: yearly_rolling_subs {
    type: number
    sql: ${TABLE}.total_rolling_yearly ;;
    hidden: no
  }
  dimension: monthly_rolling_subs{
    type: number
    sql: ${TABLE}.total_rolling_monthly ;;
    hidden: no
  }

  dimension: 30_day_rolling_churn_monthly {
    type: number
    sql: ${TABLE}.rolling_30_day_unique_user_count_monthly ;;
    hidden: no
  }

  dimension: 30_day_rolling_churn_yearly {
    type: number
    sql: ${TABLE}.rolling_30_day_unique_user_count_yearly ;;
    hidden: no
  }
}
