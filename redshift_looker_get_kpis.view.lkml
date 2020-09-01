view: redshift_looker_get_kpis {
  derived_table: {
    sql: SELECT * FROM looker.get_kpis
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: uuid {
    type: number
    sql: ${TABLE}.uuid ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: analytics_v2_timestamp_date {
    type: time
    sql: ${TABLE}.analytics_v2_timestamp_date ;;
  }

  dimension: analytics_v2_trials_by_day {
    type: number
    sql: ${TABLE}.analytics_v2_trials_by_day ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: analytics_v2_paid_a {
    type: number
    sql: ${TABLE}.analytics_v2_paid_a ;;
  }

  dimension: churn_rate {
    type: number
    sql: ${TABLE}.churn_rate ;;
  }

  dimension: ltv_cpa_cpa {
    type: number
    sql: ${TABLE}.ltv_cpa_cpa ;;
  }

  dimension: analytics_v2_conversion_rate_v2 {
    type: number
    sql: ${TABLE}.analytics_v2_conversion_rate_v2 ;;
  }

  dimension: ltv_cpa_ltv {
    type: number
    sql: ${TABLE}.ltv_cpa_ltv ;;
  }

  dimension_group: ltv_cpa_timestamp_date {
    type: time
    sql: ${TABLE}.ltv_cpa_timestamp_date ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  set: detail {
    fields: [
      id,
      received_at_time,
      uuid,
      event,
      sent_at_time,
      user_id,
      analytics_v2_timestamp_date_time,
      analytics_v2_trials_by_day,
      context_library_name,
      event_text,
      timestamp_time,
      context_app_name,
      original_timestamp_time,
      uuid_ts_time,
      context_app_version,
      context_library_version,
      analytics_v2_paid_a,
      churn_rate,
      ltv_cpa_cpa,
      analytics_v2_conversion_rate_v2,
      ltv_cpa_ltv,
      ltv_cpa_timestamp_date_time,
      anonymous_id
    ]
  }
}
