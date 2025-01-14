view: segment_consent {
    derived_table: {
      sql: select context_ip, context_consent_category_preferences_c0004, timestamp
        from javascript_upff_home.segment_consent_preference_updated ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }
    measure: distinct_ip_with_consent {
      type: count_distinct
      sql: ${context_ip} ;;
      filters: [context_consent_category_preferences_c0004: "true"]
      description: "Counts distinct IPs where context_consent_category_preferences_c0004 is true"
    }
  measure: distinct_ip {
    type: count_distinct
    sql: ${context_ip} ;;
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
