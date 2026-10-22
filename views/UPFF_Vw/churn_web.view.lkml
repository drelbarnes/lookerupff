view: churn_web {
  derived_table: {
    sql:
      SELECT
        subscription_id || '|' || event_ts::VARCHAR  AS cancellation_id,
        subscription_id,
        customer_id,
        customer_email,
        billing_period,
        CASE WHEN is_dunning THEN 'dunning' ELSE 'voluntary' END AS churn_type,
        cancel_reason_code,
        is_admin_cancel,
        card_gateway,
        event_ts,
        event_date_utc,
        event_date_plus18,
        secs_since_activation
      FROM ${chargebee_cancellations.SQL_TABLE_NAME}
      WHERE is_up
        AND activated_at_epoch IS NOT NULL
      ;;
  }

  # ---------- keys ----------

  dimension: cancellation_id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.cancellation_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: customer_email {
    type: string
    sql: ${TABLE}.customer_email ;;
  }

  # ---------- churn split ----------

  dimension: churn_type {
    description: "dunning = involuntary (payment failed, no card, fraud review, ...). voluntary = everything else."
    type: string
    sql: ${TABLE}.churn_type ;;
  }

  dimension: is_dunning {
    type: yesno
    sql: ${TABLE}.churn_type = 'dunning' ;;
  }

  dimension: cancel_reason_code {
    type: string
    sql: ${TABLE}.cancel_reason_code ;;
  }

  dimension: is_admin_cancel {
    description: "Cancelled from the Chargebee admin console rather than by the customer or the system."
    type: yesno
    sql: ${TABLE}.is_admin_cancel ;;
  }

  dimension: card_gateway {
    type: string
    sql: ${TABLE}.card_gateway ;;
  }

  # ---------- plan ----------

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }


  dimension: days_since_activation {
    description: "Days between activation and cancellation."
    type: number
    sql: ${TABLE}.secs_since_activation / 86400.0 ;;
    value_format_name: decimal_1
  }

  # ---------- dates ----------

  dimension_group: cancelled {
    description: "Cancellation webhook timestamp (UTC)."
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.event_ts ;;
  }

  dimension: report_date {
    description: "UTC date of the cancellation. Same day convention as churn and rolling_platform."
    type: date
    sql: ${TABLE}.event_date_utc ;;
  }


  # ---------- measures ----------

  measure: cancellations {
    description: "Distinct web subscriptions cancelled (dunning + voluntary)."
    type: count_distinct
    sql: ${subscription_id} ;;
  }

  measure: dunning_cancellations {
    type: count_distinct
    sql: ${subscription_id} ;;
    filters: [churn_type: "dunning"]
  }

  measure: voluntary_cancellations {
    type: count_distinct
    sql: ${subscription_id} ;;
    filters: [churn_type: "voluntary"]
  }

  measure: dunning_share {
    description: "Dunning cancellations as a share of all web cancellations."
    type: number
    sql: 1.0 * ${dunning_cancellations} / NULLIF(${cancellations}, 0) ;;
    value_format_name: percent_1
  }
}
