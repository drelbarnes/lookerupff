view: vimeo_ott_customer_record {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: WITH ranked_subscriptions AS (
        SELECT
          user_id,
          action,
          action_type,
          status,
          frequency,
          platform,
          report_date
        FROM
          customers.all_customers
        WHERE action != 'follow'
      )
      , customer_record as (
        select *
        from ${vimeo_ott_subscriber_events.SQL_TABLE_NAME}
      )
      , state_changes_p0 AS (
       SELECT
          rs.*,
          "date",
          event_timestamp,
          event,
          previous_event,
          oe.platform as event_platform,
          subscription_frequency,
          subscription_status,
          next_payment_date
        FROM
          ranked_subscriptions rs
          left join customer_record oe
          on rs.user_id = oe.user_id and rs.platform = oe.platform
          and date(oe."date") = date(rs.report_date)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
      )
      , state_changes_p1 as (
        select *,
          CASE
            WHEN status = 'enabled' AND event = 'customer_product_free_trial_expired' THEN 'expired'
            WHEN status = 'enabled' AND event = 'customer_product_charge_failed' AND previous_event = 'customer_product_free_trial_created' THEN 'free_trial'
            WHEN status = 'enabled' AND event = 'customer_product_free_trial_created' THEN 'free_trial'
            WHEN status = 'expired' AND event = 'customer_product_renewed' THEN 'enabled'
            WHEN status = 'expired' AND event = 'customer_product_charge_failed' and previous_event = 'customer_product_renewed' THEN 'enabled'
            ELSE status
          END AS adjusted_status
        from state_changes_p0
      )
      , state_changes_p2 AS (
        select *
        , LAG(adjusted_status) OVER (PARTITION BY user_id ORDER BY report_date, event_timestamp) AS prev_adjusted_status
        from state_changes_p1
      )
      , state_changes AS (
        SELECT
          *,
          CASE
            WHEN adjusted_status = 'free_trial' AND (prev_adjusted_status IS NULL OR prev_adjusted_status in ('enabled', 'cancelled', 'disabled', 'expired', 'refunded')) THEN 'free_trial_created'
            WHEN adjusted_status = 'enabled' AND prev_adjusted_status = 'free_trial' AND event != 'customer_product_free_trial_expired' THEN 'free_trial_converted'
            WHEN (adjusted_status in ('cancelled', 'disabled', 'expired', 'refunded') AND prev_adjusted_status = 'free_trial') OR (adjusted_status = 'enabled' AND prev_adjusted_status = 'free_trial' AND event = 'customer_product_free_trial_expired') THEN 'free_trial_churn'
            WHEN adjusted_status = 'enabled' AND (prev_adjusted_status IS NULL OR prev_adjusted_status NOT IN ('enabled', 'free_trial')) THEN 'paying_created'
            WHEN adjusted_status in ('cancelled', 'disabled', 'expired', 'refunded') AND prev_adjusted_status = 'enabled' THEN 'paying_churn'
            WHEN adjusted_status = 'paused' AND prev_adjusted_status <> 'paused' THEN 'paused_created'
            ELSE null::VARCHAR
          END AS state_change
        FROM
        state_changes_p2
      )
      select * from state_changes
      ;;
    datagroup_trigger: upff_event_processing
    distribution_style: all
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: report_date {
    type: time
    timeframes: [date, week, month]
    sql: ${TABLE}.report_date ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.date ;;
  }

  dimension_group: event_timestamp {
    type: time
    sql: ${TABLE}.event_timestamp ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: previous_event {
    type: string
    sql: ${TABLE}.previous_event ;;
  }

  dimension: event_platform {
    type: string
    sql: ${TABLE}.event_platform ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: adjusted_status {
    type: string
    sql: ${TABLE}.adjusted_status ;;
  }

  dimension: prev_adjusted_status {
    type: string
    sql: ${TABLE}.prev_adjusted_status ;;
  }

  dimension: state_change {
    type: string
    sql: ${TABLE}.state_change ;;
  }

  dimension_group: next_payment_date {
    type: time
    sql: ${TABLE}.next_payment_date ;;
  }
}
