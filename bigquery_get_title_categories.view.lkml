view: bigquery_get_title_categories {
  derived_table: {
    sql: with a as
      (select max(sent_at) as sent_at
      from php.get_title_categories)

      (select b.*
      from php.get_title_categories as b inner join a on date(a.sent_at)=date(b.sent_at))
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: cat_id {
    type: number
    sql: ${TABLE}.cat_id ;;
  }

  dimension: cat_order {
    type: number
    sql: ${TABLE}.cat_order ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension_group: created_at {
    type: time
    sql: ${TABLE}.created_at ;;
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

  dimension_group: ingest_date {
    type: time
    sql: ${TABLE}.ingest_date ;;
  }

  dimension: is_automatic {
    type: string
    sql: ${TABLE}.is_automatic ;;
  }

  dimension: is_available {
    type: string
    sql: ${TABLE}.is_available ;;
  }

  dimension: is_featured {
    type: string
    sql: ${TABLE}.is_featured ;;
  }

  dimension: items_count {
    type: number
    sql: ${TABLE}.items_count ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension_group: updated_at {
    type: time
    sql: ${TABLE}.updated_at ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension_group: ingest_at {
    type: time
    sql: ${TABLE}.ingest_at ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
  }

  set: detail {
    fields: [
      cat_id,
      cat_order,
      context_library_consumer,
      context_library_name,
      context_library_version,
      created_at_time,
      event,
      event_text,
      id,
      ingest_date_time,
      is_automatic,
      is_available,
      is_featured,
      items_count,
      loaded_at_time,
      name,
      original_timestamp_time,
      received_at_time,
      sent_at_time,
      timestamp_time,
      updated_at_time,
      user_id,
      uuid_ts_time,
      ingest_at_time,
      thumbnail
    ]
  }
}
