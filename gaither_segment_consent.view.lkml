view: gaither_segment_consent {
  derived_table: {
    sql: select *
      from javascript_gaither_tv.segment_consent_preference_updated ;;
  }

  measure: distinct_ip_c0001 {
    type: count_distinct
    sql: ${context_ip} ;;
    filters: [context_consent_category_preferences_c0001: "true"]
    label: "Unique IP count 0001 consent"
    description: "Counts distinct IPs where context_consent_category_preferences_c0001 is true"
  }
  measure: distinct_ip_c0002 {
    type: count_distinct
    sql: ${context_ip} ;;
    filters: [context_consent_category_preferences_c0002: "true"]
    label: "Unique IP count 0002 consent"
    description: "Counts distinct IPs where context_consent_category_preferences_c0002 is true"
  }
  measure: distinct_ip_c0003 {
    type: count_distinct
    sql: ${context_ip} ;;
    filters: [context_consent_category_preferences_c0003: "true"]
    label: "Unique IP count 0003 consent"
    description: "Counts distinct IPs where context_consent_category_preferences_c0003 is true"
  }
  measure: distinct_ip_c0004 {
    type: count_distinct
    sql: ${context_ip} ;;
    filters: [context_consent_category_preferences_c0004: "true"]
    label: "Unique IP count 0004 consent"
    description: "Counts distinct IPs where context_consent_category_preferences_c0004 is true"
  }
  measure: distinct_ip {
    type: count_distinct
    sql: ${context_ip} ;;
    label: "Unique IP count"
    description: "Counts distinct IP address"
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }
  dimension: context_consent_category_preferences_c0001 {
    type: string
    sql: ${TABLE}.context_consent_category_preferences_c0001 ;;
  }
  dimension: context_consent_category_preferences_c0002 {
    type: string
    sql: ${TABLE}.context_consent_category_preferences_c0002 ;;
  }
  dimension: context_consent_category_preferences_c0003 {
    type: string
    sql: ${TABLE}.context_consent_category_preferences_c0003 ;;
  }
  dimension: context_consent_category_preferences_c0004 {
    type: string
    sql: ${TABLE}.context_consent_category_preferences_c0004 ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  set: detail {
    fields: [
      context_ip,
      context_consent_category_preferences_c0001,
      context_consent_category_preferences_c0002,
      context_consent_category_preferences_c0003,
      context_consent_category_preferences_c0004,
      timestamp_time
    ]
  }
}
