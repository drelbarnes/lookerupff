view: bango_events_em {
  derived_table: {
    sql: select* from looker.bango_events
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

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_protocols_source_id {
    type: string
    sql: ${TABLE}.context_protocols_source_id ;;
  }

  dimension_group: date_activated {
    type: time
    sql: ${TABLE}.date_activated ;;
  }

  dimension: immediate {
    type: string
    sql: ${TABLE}.immediate ;;
  }

  dimension: reseller_key {
    type: string
    sql: ${TABLE}.reseller_key ;;
  }

  dimension: bango_user_id {
    type: number
    sql: ${TABLE}.bango_user_id ;;
  }

  dimension: date_created {
    type: string
    sql: ${TABLE}.date_created ;;
  }

  dimension: product_key {
    type: string
    sql: ${TABLE}.product_key ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: bango_status {
    type: string
    sql: ${TABLE}.bango_status ;;
  }

  dimension: date_ended {
    type: string
    sql: ${TABLE}.date_ended ;;
  }

  dimension: date_suspended {
    type: string
    sql: ${TABLE}.date_suspended ;;
  }

  dimension: entitlement_id {
    type: string
    sql: ${TABLE}.entitlement_id ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: partner {
    type: string
    sql: ${TABLE}.partner ;;
  }

  dimension: partner_id {
    type: number
    sql: ${TABLE}.partner_id ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension_group: event_date {
    type: time
    sql: ${TABLE}.event_date ;;
  }

  dimension: statuses_id {
    type: number
    sql: ${TABLE}.statuses_id ;;
  }

  dimension: event_id {
    type: number
    sql: ${TABLE}.event_id ;;
  }

  set: detail {
    fields: [
      id,
      received_at_time,
      uuid,
      context_app_version,
      context_library_name,
      context_protocols_source_id,
      date_activated_time,
      immediate,
      reseller_key,
      bango_user_id,
      date_created,
      product_key,
      timestamp_time,
      user_id,
      context_app_name,
      event,
      original_timestamp_time,
      sent_at_time,
      bango_status,
      date_ended,
      date_suspended,
      entitlement_id,
      event_text,
      partner,
      partner_id,
      context_library_version,
      uuid_ts_time,
      event_date_time,
      statuses_id,
      event_id
    ]
  }
}
