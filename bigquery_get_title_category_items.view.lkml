view: bigquery_get_title_category_items {
  derived_table: {
    sql: with b as
      (select max(sent_at) as sent_at
      from php.get_title_category_items)

      (select a.*
      from php.get_title_category_items as a inner join b on date(a.sent_at)=date(b.sent_at))
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

  dimension: duration_formatted {
    type: string
    sql: ${TABLE}.duration_formatted ;;
  }

  dimension: duration_seconds {
    type: number
    sql: ${TABLE}.duration_seconds ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: has_free_videos {
    type: string
    sql: ${TABLE}.has_free_videos ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: ingest_at {
    type: time
    sql: ${TABLE}.ingest_at ;;
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

  dimension: item_id {
    type: number
    sql: ${TABLE}.item_id ;;
  }

  dimension: item_order {
    type: number
    sql: ${TABLE}.item_order ;;
  }

  dimension: items_count {
    type: number
    sql: ${TABLE}.items_count ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: metadata_cast {
    type: string
    sql: ${TABLE}.metadata_cast ;;
  }

  dimension: metadata_primary_genre {
    type: string
    sql: ${TABLE}.metadata_primary_genre ;;
  }

  dimension: metadata_released_year {
    type: number
    sql: ${TABLE}.metadata_released_year ;;
  }

  dimension: metadata_secondary_genre {
    type: string
    sql: ${TABLE}.metadata_secondary_genre ;;
  }

  dimension: metadata_studio {
    type: string
    sql: ${TABLE}.metadata_studio ;;
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

  dimension: seasons_count {
    type: number
    sql: ${TABLE}.seasons_count ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
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

  dimension: videos_count {
    type: number
    sql: ${TABLE}.videos_count ;;
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
      duration_formatted,
      duration_seconds,
      event,
      event_text,
      has_free_videos,
      id,
      ingest_at_time,
      is_automatic,
      is_available,
      is_featured,
      item_id,
      item_order,
      items_count,
      loaded_at_time,
      metadata_cast,
      metadata_primary_genre,
      metadata_released_year,
      metadata_secondary_genre,
      metadata_studio,
      name,
      original_timestamp_time,
      received_at_time,
      seasons_count,
      sent_at_time,
      short_description,
      timestamp_time,
      type,
      updated_at_time,
      user_id,
      uuid_ts_time,
      videos_count,
      thumbnail
    ]
  }
}
