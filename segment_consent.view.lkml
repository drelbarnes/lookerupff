view: segment_consent {
    derived_table: {
      sql: *
        from javascript_upff_home.segment_consent_preference_updated ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }
    measure: distinct_ip_with_target_cookie_consent {
      type: count_distinct
      sql: ${context_ip} ;;
      filters: [context_consent_category_preferences_c0004: "true"]
      label: "Unique IP count with target cookie consent"
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
        context_consent_category_preferences_c0004,
        timestamp_time
      ]
    }
  }
