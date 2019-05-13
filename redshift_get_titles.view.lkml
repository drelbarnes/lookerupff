view: redshift_get_titles {
  sql_table_name: php.get_titles ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
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

  dimension_group: created {
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
    sql: ${TABLE}.created_at ;;
  }

  dimension: duration_formatted {
    type: string
    sql: ${TABLE}.duration_formatted ;;
  }

  dimension: duration_millisecond {
    type: number
    sql: ${TABLE}.duration_millisecond ;;
  }

  dimension: duration_seconds {
    type: number
    sql: ${TABLE}.duration_seconds ;;
  }

  dimension: episode_number {
    type: number
    sql: ${TABLE}.episode_number ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: ingest {
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
    sql: ${TABLE}.ingest_at ;;
  }

  dimension: is_available {
    type: yesno
    sql: ${TABLE}.is_available ;;
  }

  dimension: is_free {
    type: yesno
    sql: ${TABLE}.is_free ;;
  }

  dimension: metadata_episode_number {
    type: number
    sql: ${TABLE}.metadata_episode_number ;;
  }

  dimension: metadata_movie_name {
    type: string
    sql: ${TABLE}.metadata_movie_name ;;
  }

  dimension: metadata_primary_genre {
    type: string
    sql: ${TABLE}.metadata_primary_genre ;;
  }

  dimension: metadata_season_name {
    type: string
    sql: ${TABLE}.metadata_season_name ;;
  }

  dimension: metadata_season_number {
    type: number
    sql: ${TABLE}.metadata_season_number ;;
  }

  dimension: metadata_secondary_genre {
    type: string
    sql: ${TABLE}.metadata_secondary_genre ;;
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

  dimension_group: received {
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

  dimension: season_number {
    type: number
    sql: ${TABLE}.season_number ;;
  }

  dimension_group: sent {
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

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
  }

  dimension: title_image {
    type: string
    sql: ${thumbnail};;
    html: <img src="{{thumbnail}}" width="150" /> ;;
  }

  dimension_group: time_available {
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
    sql: ${TABLE}.time_available ;;
  }

  dimension_group: time_unavailable {
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
    sql: ${TABLE}.time_unavailable ;;
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

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension_group: updated {
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
    sql: ${TABLE}.updated_at ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
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

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  measure: count {
    type: count
    drill_fields: [id, context_library_name, metadata_season_name, metadata_movie_name]
  }
}
