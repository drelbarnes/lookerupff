view: my_aspireapp_installs {
    sql_table_name: aspire_app.application_installed ;;

    dimension: anonymous_id {
      type: string
      sql: ${TABLE}.anonymous_id ;;
    }

    measure: total_anonymous_ids {
      type: count_distinct
      sql: ${anonymous_id} ;;
      description: "Total number of anonymous users."
    }

    dimension: build {
      type: number
      sql: ${TABLE}.build ;;
    }

    dimension: context_app_build {
      type: number
      sql: ${TABLE}.context_app_build ;;
    }

    dimension: context_app_name {
      type: string
      sql: ${TABLE}.context_app_name ;;
    }

    dimension: context_app_namespace {
      type: string
      sql: ${TABLE}.context_app_namespace ;;
    }

    dimension: context_app_version {
      type: string
      sql: ${TABLE}.context_app_version ;;
    }

    dimension: context_device_manufacturer {
      type: string
      sql: ${TABLE}.context_device_manufacturer ;;
    }
    dimension: context_device_model {
      type: string
      sql: ${TABLE}.context_device_model ;;
    }
    dimension: context_device_name {
      type: string
      sql: ${TABLE}.context_device_name ;;
    }

    dimension: context_device_type {
      type: string
      sql: ${TABLE}.context_device_type ;;
    }

    dimension: context_ip {
      type: string
      sql: ${TABLE}.context_ip ;;
    }

    dimension: context_library_name {
      type: string
      sql: ${TABLE}.context_library_name ;;
    }

    dimension: context_library_version {
      type: string
      sql: ${TABLE}.context_library_version ;;
    }
    dimension: context_locale {
      type: string
      sql: ${TABLE}.context_locale ;;
    }

    dimension: context_network_cellular {
      type: string
      sql: ${TABLE}.context_network_cellular ;;
    }

    dimension: context_network_wifi {
      type: string
      sql: ${TABLE}.context_network_wifi ;;
    }

    dimension: context_os_name {
      type: string
      sql: ${TABLE}.context_os_name ;;
    }

    dimension: context_os_version {
      type: string
      sql: ${TABLE}.context_os_version ;;
    }

    dimension: context_screen_height {
      type: number
      sql: ${TABLE}.context_screen_height ;;
    }

    dimension: context_screen_width {
      type: number
      sql: ${TABLE}.context_screen_width ;;
    }

    dimension: context_timezone {
      type: string
      sql: ${TABLE}.context_timezone ;;
    }

    dimension: event {
      type: string
      sql: ${TABLE}.event ;;
    }

    dimension: event_text {
      type: string
      sql: ${TABLE}.event_text ;;
    }

    dimension: id {
      type: string
      sql: ${TABLE}.id ;;
    }

    dimension: loaded_at {
      type: string
      sql: ${TABLE}.loaded_at ;;
    }

    dimension_group: original_timestamp {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.original_timestamp ;;
    }

    dimension_group: received_at {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.received_at ;;
    }


    dimension_group: sent_at {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.sent_at ;;
    }

    dimension_group: timestamp {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.timestamp ;;
    }

    dimension_group: uuid_ts {
      type: time
      timeframes: [
        raw,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.uuid_ts ;;
    }

    dimension: version {
      type: string
      sql: ${TABLE}.version ;;
    }

    dimension: context_screen_density {
      type: string
      sql: ${TABLE}.context_screen_density ;;
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: context_protocols_source_id {
      type: string
      sql: ${TABLE}.context_protocols_source_id ;;
    }

    dimension: context_instant_id {
      type: string
      sql: ${TABLE}.context_instant_id ;;
    }
  }
