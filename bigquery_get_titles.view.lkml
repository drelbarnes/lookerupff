view: bigquery_get_titles {
  derived_table: {
    sql: with c as
      (select max(sent_at) as sent_at
      from php.get_titles),

      get_titles as
      (select a.*
      from php.get_titles as a inner join c on date(a.sent_at)=date(c.sent_at)),

      titles_id_mapping as
      (select *
      from svod_titles.titles_id_mapping
      where collection not in ('Romance - OLD',
      'Dramas',
      'Comedies',
      'Kids - OLD',
      'Christmas',
      'Just Added',
      'Music',
      'Faith Movies',
      'Docs & Specials',
      'Trending',
      'Adventure',
      'All Movies',
      'All Series',
      'Bonus Content',
      'Drama Movies',
      'Drama Series',
      'Faith Favorites',
      'Family Addition',
      'Family Comedies',
      'Fan Favorite Series',
      'Fantasy',
      'Kids',
      'New',
      'New Series',
      'Romance',
      'Sports',
      'The Must-Watch List',
      'UPlifting Reality',
      'UP Original Movies and Series',
      'UP Original Series'
      ))

      select a.*,
             b.collection
      from get_titles as a inner join titles_id_mapping as b on a.title=b.title
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: ingest_at {
    type: time
    sql: ${TABLE}.ingest_at ;;
  }

  dimension: is_available {
    type: string
    sql: ${TABLE}.is_available ;;
  }

  dimension: is_free {
    type: string
    sql: ${TABLE}.is_free ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: metadata_episode_number {
    type: number
    sql: ${TABLE}.metadata_episode_number ;;
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
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: season_number {
    type: number
    sql: ${TABLE}.season_number ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
    html: <img src="{{value}}"/> ;;
  }

  dimension_group: time_available {
    type: time
    sql: ${TABLE}.time_available ;;
  }

  dimension_group: time_unavailable {
    type: time
    sql: ${TABLE}.time_unavailable ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
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

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  set: detail {
    fields: [
      context_library_consumer,
      context_library_name,
      context_library_version,
      created_at_time,
      duration_formatted,
      duration_millisecond,
      duration_seconds,
      episode_number,
      event,
      event_text,
      id,
      ingest_at_time,
      is_available,
      is_free,
      loaded_at_time,
      metadata_episode_number,
      metadata_primary_genre,
      metadata_season_name,
      metadata_season_number,
      metadata_secondary_genre,
      original_timestamp_time,
      received_at_time,
      season_number,
      sent_at_time,
      short_description,
      thumbnail,
      time_available_time,
      time_unavailable_time,
      timestamp_time,
      title,
      updated_at_time,
      user_id,
      uuid_ts_time,
      video_id,
      collection
    ]
  }
}
