view: segment_consent {
    derived_table: {
      sql: select context_ip, context_consent_category_preferences_c0004, timestamp
        from javascript_upff_home.segment_consent_preference_updated ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
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
