view: redshift_looker_get_titles {
  derived_table: {
    sql:
    with p0 as (
      SELECT
      title
      , "timestamp"
      , video_id
      , is_available
      , media_type
      , metadata_episode_number
      , metadata_movie_name
      , metadata_primary_genre
      , metadata_season_name
      , metadata_season_number
      , metadata_secondary_genre
      , metadata_series_name
      , plans
      , plays_count
      , row_number() over (partition by video_id order by timestamp desc) as n
      FROM php.get_titles
    )
    select
    title
    , "timestamp"
    , video_id
    , is_available
    , media_type
    , metadata_episode_number
    , metadata_movie_name
    , metadata_primary_genre
    , metadata_season_name
    , metadata_season_number
    , metadata_secondary_genre
    , metadata_series_name
    , plans
    , plays_count
    from p0
    where n=1
    ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: is_available {
    type: yesno
    sql: ${TABLE}.is_available ;;
  }

  dimension: media_type {
    type: string
    sql: ${TABLE}.media_type ;;
  }

  dimension: metadata_episode_number {
    type: string
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
    type: string
    sql: ${TABLE}.metadata_season_number ;;
  }

  dimension: metadata_secondary_genre {
    type: string
    sql: ${TABLE}.metadata_secondary_genre ;;
  }

  dimension: metadata_series_name {
    type: string
    sql: ${TABLE}.metadata_series_name ;;
  }

  dimension: plans {
    type: string
    sql: ${TABLE}.plans ;;
  }

  dimension: plays_count {
    type: number
    sql: ${TABLE}.plays_count ;;
  }

  set: detail {
    fields: [title, video_id, timestamp_time]
  }
}
