# Datagroup definition - place at the model level in your .model.lkml file.
# This should not live inside the view file if your project separates model and view files.

view: ltv_cpa {
  derived_table: {

    datagroup_trigger: ltv_cpa_datagroup
    increment_key: "report_date"
    increment_offset: 7
    distribution: "report_date"
    sortkeys: ["report_date"]

    sql:
      , v2_table AS (
        SELECT *
        FROM ${UPFF_analytics_Vw_v2.SQL_TABLE_NAME}
        WHERE report_date >= '2025-12-30'
          AND {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      cancelled_user AS (
      SELECT *
      FROM ${churn.SQL_TABLE_NAME}
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
      ),

      spend AS (
      SELECT
      date_start AS report_date,
      SUM(spend) AS spend
      FROM ${daily_spend.SQL_TABLE_NAME}
      WHERE date_start >= '2025-12-30'
      AND {% incrementcondition %} date_start {% endincrementcondition %}
      GROUP BY 1
      ),

      rolling_spend AS (
      SELECT
      report_date,
      SUM(spend) OVER (
      ORDER BY report_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ) AS rolling_spend
      FROM spend
      ),

      trials_converted AS (
      SELECT *
      FROM ${trial_converted.SQL_TABLE_NAME}
      WHERE {% incrementcondition %} report_date {% endincrementcondition %}
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
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
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
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
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
      AND {% incrementcondition %} report_date {% endincrementcondition %}
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
      a.report_date
      FROM prior_subs b
      LEFT JOIN rolling_churn a
      ON a.report_date = b.report_date
      AND a.platform = b.platform
      AND a.billing_period = b.billing_period
      ),

      cpa AS (
      SELECT
      a.report_date,
      a.rolling_spend,
      b.rolling_converted
      FROM rolling_spend a
      LEFT JOIN rolling_converted b
      ON a.report_date = b.report_date
      )

      SELECT
      a.rolling_churn_30_days,
      a.prior_31_days_subs,
      a.platform,
      a.billing_period,
      a.report_date,
      b.rolling_spend,
      b.rolling_converted
      FROM result a
      LEFT JOIN cpa b
      ON a.report_date = b.report_date
      ;;

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
datagroup: ltv_cpa_datagroup {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '10 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}
