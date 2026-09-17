view: churn_gain {
  derived_table: {


    sql:
      SELECT
        CAST(user_count     AS BIGINT)  AS user_count,
        CAST(report_date    AS DATE)    AS report_date,
        CAST(billing_period AS VARCHAR) AS billing_period,
        CAST(status         AS VARCHAR) AS status,
        CAST(platform       AS VARCHAR) AS platform
      FROM (

      SELECT
      SUM(user_count) AS user_count,
      report_date,
      billing_period,
      status,
      platform
      FROM (

      SELECT user_count, report_date, billing_period, platform, 'churn'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (
      content_subscription_cancel_reason_code NOT IN (
      'Not Paid', 'No Card', 'Fraud Review Failed',
      'Non Compliant EU Customer', 'Tax Calculation Failed',
      'Currency incompatible with Gateway', 'Non Compliant Customer'
      )
      OR content_subscription_cancel_reason_code IS NULL
      )
      AND content_subscription_activated_at IS NOT NULL
      AND content_subscription_subscription_items LIKE '%UP%'

      UNION ALL

      SELECT
      a.user_id,
      b.billing_period,
      a.report_date,
      b.platform
      FROM (
      SELECT DATE("timestamp") AS report_date, CAST(user_id AS VARCHAR) AS user_id
      FROM vimeo_ott_webhook.customer_product_expired
      ) a
      LEFT JOIN (
      SELECT report_date, user_id, billing_period, platform
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      ) b
      ON a.report_date = b.report_date AND a.user_id = b.user_id
      ) churn_src
      GROUP BY 2, 3, 4
      ) cancelled_user_count

      UNION ALL

      SELECT user_count, report_date, billing_period, platform, 'paused'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_paused
      WHERE content_subscription_subscription_items LIKE '%UP%'
      ) paused_pre
      GROUP BY 2, 3, 4
      ) paused_count

      UNION ALL

      SELECT user_count, report_date, billing_period, platform, 'converted'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      DATE(received_at)                        AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_activated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      and (content_subscription_activated_at-content_subscription_created_at)<864000

      UNION ALL

      SELECT
      b.report_date,
      b.user_id,
      b.billing_period,
      a.platform
      FROM (
      SELECT report_date, user_id, billing_period, platform
      FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      ) a
      RIGHT JOIN (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR)     AS user_id,
      subscription_frequency::VARCHAR  AS billing_period,
      event_type::VARCHAR              AS event_type,
      DATE(event_occurred_at)          AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      ) b
      ON a.report_date = b.report_date AND a.user_id = b.user_id
      WHERE b.event_type = 'Free Trial to Paid'
      ) conversion_src
      GROUP BY 2, 3, 4
      ) conversion_count

      UNION ALL

      SELECT user_count, report_date, billing_period, platform, 'reacquisition'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_reactivated
      WHERE content_subscription_subscription_items LIKE '%UP%'
      and content_subscription_status != 'in_trial'

      UNION ALL

      SELECT
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_resumed
      WHERE content_subscription_subscription_items LIKE '%UP%'

      UNION ALL
/*
      SELECT
      b.report_date,
      b.user_id,
      b.billing_period,
      a.platform
      FROM (
      SELECT report_date, user_id, billing_period, platform
      FROM {UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      ) a
      RIGHT JOIN (
      SELECT DISTINCT
      CAST(customer_id AS VARCHAR)     AS user_id,
      subscription_frequency::VARCHAR  AS billing_period,
      event_type::VARCHAR              AS event_type,
      DATE(event_occurred_at)          AS report_date
      FROM customers.new_customers
      WHERE subscription_frequency != 'custom'
      ) b
      ON a.report_date = b.report_date AND a.user_id = b.user_id
      WHERE b.event_type = 'Direct to Paid' */
      select
      report_date,
        user_id,
        subscription_frequency as billing_period,
        platform
    from (
        select
            user_id,
            subscription_frequency,
            platform,
            DATE(DATEADD(HOUR, -4, timestamp)) AS report_date,
            created_at,
            row_number() over (
                partition by user_id
                order by report_date
            ) as rn
        from vimeo_ott_webhook.customer_product_created
        where platform != 'api'
          --and date(timestamp) = date(created_at)
    )
    where rn != 1
      ) reacq_src
      GROUP BY 2, 3, 4
      ) re_acquisition_count

      UNION ALL

      SELECT user_count, report_date, billing_period, platform, 'dunning'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
      content_subscription_id::VARCHAR         AS user_id,
      CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END                                      AS billing_period,
      DATE(DATEADD(HOUR, +18, timestamp))      AS report_date,
      'web'::VARCHAR                           AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason_code IN (
      'Not Paid', 'No Card', 'Fraud Review Failed',
      'Non Compliant EU Customer', 'Tax Calculation Failed',
      'Currency incompatible with Gateway', 'Non Compliant Customer'
      )
      AND (content_subscription_cancelled_at - content_subscription_activated_at) > 10000
      AND content_subscription_subscription_items LIKE '%UP%'
      ) dunning_pre
      GROUP BY 2, 3, 4
      ) dunning_count

      UNION ALL

      SELECT user_count, report_date, billing_period, platform, 'new_paid'::VARCHAR AS status
      FROM (
      SELECT COUNT(DISTINCT user_id) AS user_count, report_date, billing_period, platform
      FROM (
      SELECT
        content_subscription_id as user_id
        ,CASE
        WHEN content_subscription_billing_period_unit ='month' THEN 'monthly'
        ELSE 'yearly'
        END AS billing_period
        ,'web' as platform
        ,date(DATEADD(HOUR, -4, received_at)) as report_date
        FROM chargebee_webhook_events.subscription_created
        WHERE content_subscription_subscription_items like '%UP%'

UNION ALL
    select
        user_id,
        subscription_frequency as billing_period,
        platform,
       report_date
    from (
        select
            user_id,
            subscription_frequency,
            platform,
            date(timestamp) as report_date,
            created_at,
            row_number() over (
                partition by user_id
                order by report_date
            ) as rn
        from vimeo_ott_webhook.customer_product_created
        where platform != 'api'
          --and date(timestamp) = date(created_at)
    )
    where rn = 1

      ) new_paid_pre
      GROUP BY 2, 3, 4
      ) new_paid_count

      ) result
      GROUP BY 2, 3, 4, 5

      UNION ALL

      SELECT
      CAST(rolling_30_day_unique_user_count_monthly AS BIGINT) AS user_count,
      CAST(report_date AS DATE)    AS report_date,
      'monthly'::VARCHAR           AS billing_period,
      'rolling_churn'::VARCHAR     AS status,
      CAST(platform AS VARCHAR)    AS platform
      FROM ${rolling_platform.SQL_TABLE_NAME}

      UNION ALL

      SELECT
      CAST(total_rolling_monthly AS BIGINT) AS user_count,
      CAST(report_date AS DATE)    AS report_date,
      'monthly'::VARCHAR           AS billing_period,
      'rolling_total'::VARCHAR     AS status,
      CAST(platform AS VARCHAR)    AS platform
      FROM ${rolling_platform.SQL_TABLE_NAME}

      UNION ALL

      SELECT
      CAST(rolling_30_day_unique_user_count_yearly AS BIGINT) AS user_count,
      CAST(report_date AS DATE)    AS report_date,
      'yearly'::VARCHAR            AS billing_period,
      'rolling_churn'::VARCHAR     AS status,
      CAST(platform AS VARCHAR)    AS platform
      FROM ${rolling_platform.SQL_TABLE_NAME}

      UNION ALL

      SELECT
      CAST(total_rolling_yearly AS BIGINT) AS user_count,
      CAST(report_date AS DATE)    AS report_date,
      'yearly'::VARCHAR            AS billing_period,
      'rolling_total'::VARCHAR     AS status,
      CAST(platform AS VARCHAR)    AS platform
      FROM ${rolling_platform.SQL_TABLE_NAME}

      ) all_rows
      WHERE 1=1
      --{% incrementcondition %} report_date {% endincrementcondition %}
      ;;
    sql_trigger_value:
    SELECT TO_CHAR(
    CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()) - INTERVAL '7 hour',
    'YYYY-MM-DD'
    ) ;;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
      distribution: "report_date"
      sortkeys: ["report_date"]
    }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  measure: churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "churn"]
  }

  measure: paused_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "paused"]
  }

  measure: dunning_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "dunning"]
  }

  measure: converted_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "converted"]
  }

  measure: reacquisition_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "reacquisition"]
  }

  measure: new_paid_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "new_paid"]
  }

  measure: rolling_churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_churn"]
  }

  measure: rolling_total_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_total"]
  }
}
