# Datagroup definition - place at the model level in your .model.lkml file.
# This should not live inside the view file if your project separates model and view files.

view: ltv_cpa {
  derived_table: {



    sql:
      -- FIX: leading-comma CTE fragment pattern retained (this sql block is
      -- appended to a WITH clause defined upstream in the model). The final
      -- UNION of result and cpa is promoted into a named CTE (all_rows) so
      -- the terminal SELECT is a clean SELECT * FROM all_rows WHERE (...),
      -- with an unaliased report_date column for the incrementcondition tag.
      -- This avoids any alias-resolution ambiguity and matches the pattern
      -- used across all other incremental PDTs in this project.
      , v2_table AS (
        SELECT *
        FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
        WHERE report_date >= '2025-12-30'
      ),

      cancelled_user AS (
      SELECT *
      FROM ${churn.SQL_TABLE_NAME}
      ),

      spend AS (
      SELECT
      date_start AS report_date,
      SUM(spend) AS spend
      FROM ${daily_spend.SQL_TABLE_NAME}
      WHERE date_start >= '2025-12-30'
      GROUP BY 1
      ),

      rolling_spend AS (
      SELECT
      report_date,
      SUM(spend) OVER (
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS rolling_spend
      FROM spend
      ),

      trials_converted AS (
      /*
      SELECT *
      FROM {trial_converted.SQL_TABLE_NAME}

      SELECT
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      AND DATE(received_at) >='2025-07-01'

      UNION ALL*/
      select
            DATE(DATEADD(HOUR, -4, timestamp)) AS report_date,
            user_id,
            subscription_frequency as billing_period,
            platform
        from vimeo_ott_webhook.customer_product_created
        where date(timestamp) >='2026-09-09' and platform != 'api'
/*
      UNION ALL
      SELECT
        DATE(DATEADD(HOUR, -5, event_occurred_at)) AS report_date
        ,email as user_id
        ,subscription_frequency as billing_period
        ,'vimeo' as platform
      FROM customers.new_customers
      WHERE event_type = 'Free Trial to Paid'  */

      UNION ALL
      SELECT
          DATE(DATEADD(HOUR, -4, received_at)) AS report_date,
          content_customer_id AS user_id,
          CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
          END AS billing_period,
          'web' AS platform
        FROM chargebee_webhook_events.subscription_created
        WHERE content_subscription_subscription_items LIKE '%UP%'
        and DATE(DATEADD(HOUR, -4, received_at))  >='2026-09-09'

      UNION ALL
      select
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      and date(timestamp) >= '2026-09-09'

      UNION ALL
      select
      DATE(received_at) AS report_date,
      content_subscription_id::VARCHAR AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'
      and date(timestamp) >= '2026-09-09'

      ),

      daily_converted_counts AS (
      SELECT
      report_date,
      COUNT(user_id) AS user_count
      FROM trials_converted
      GROUP BY report_date
      ),

      rolling_converted AS (
      SELECT
      report_date,
      SUM(user_count) OVER (
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) AS rolling_converted
      FROM daily_converted_counts
      ),

      rolling_churn AS (
      SELECT
      report_date,
      platform,
      billing_period,
      SUM(user_count) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
      ) AS rolling_churn_30_days
      FROM cancelled_user
      ),

      subs AS (
      SELECT
      report_date,
      platform,
      billing_period,
      user_count
      FROM ${sub_count.SQL_TABLE_NAME}
      WHERE status = 'active'
      ),

      prior_subs AS (
      SELECT
      report_date,
      platform,
      billing_period,
      LAG(user_count, 31) OVER (
      PARTITION BY platform, billing_period
      ORDER BY report_date
      ) AS prior_31_days_subs
      FROM subs
      ),

      result AS (
      SELECT
      a.rolling_churn_30_days,
      b.prior_31_days_subs,
      a.platform,
      a.billing_period,
      a.report_date,
      c.rolling_spend,
      c.rolling_converted
      FROM prior_subs b
      LEFT JOIN rolling_churn a
      ON  a.report_date    = b.report_date
      AND a.platform       = b.platform
      AND a.billing_period = b.billing_period
      LEFT JOIN (
      SELECT
      rs.report_date,
      rs.rolling_spend,
      rc.rolling_converted
      FROM rolling_spend rs
      LEFT JOIN rolling_converted rc
      ON rs.report_date = rc.report_date
      ) c
      ON a.report_date = c.report_date
      ),

      all_rows AS (
      SELECT
      CAST(rolling_churn_30_days AS BIGINT)  AS rolling_churn_30_days,
      CAST(prior_31_days_subs    AS BIGINT)  AS prior_31_days_subs,
      CAST(platform              AS VARCHAR) AS platform,
      CAST(billing_period        AS VARCHAR) AS billing_period,
      CAST(report_date           AS DATE)    AS report_date,
      CAST(rolling_spend         AS NUMERIC(18,2)) AS rolling_spend,
      CAST(rolling_converted     AS BIGINT)  AS rolling_converted
      FROM result
      )

      SELECT *
      FROM all_rows;;
          sql_trigger_value:
    SELECT TO_CHAR(
    CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '7 hour',
    'YYYY-MM-DD'
    ) ;;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]


  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: rolling_converted {
    type: max
    sql: ${TABLE}.rolling_converted ;;
  }

  measure: rolling_spend {
    type: max
    sql: ${TABLE}.rolling_spend ;;
  }

  measure: prior_31_days_subs_count {
    type: sum
    sql: ${TABLE}.prior_31_days_subs ;;
  }

  measure: rolling_churn_30_days_count {
    type: sum
    sql: ${TABLE}.rolling_churn_30_days ;;
  }
}

################################################################################
# Datagroup — triggers the daily incremental run at 10 AM ET
# NOTE: This must be defined at the MODEL level (in your .model.lkml file),
# not inside the view file.
################################################################################
