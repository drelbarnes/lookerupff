view: bigquery_get_most_recent_titles {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: with p0 as (
        SELECT
        *
        , row_number() over (partition by video_id, is_available order by timestamp desc) as n
        FROM php.get_titles
      )
      select
      timestamp
      , title
      , video_id
      , media_type
      , metadata_series_name as series
      , metadata_season_name as season_collection_name
      , metadata_movie_name as movie_collection_name
      , case
        when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
        when metadata_season_name is null then metadata_movie_name
        else metadata_season_name
      end as collection
      , metadata_season_number as season
      , metadata_episode_number as episode
      , date(time_available) as date
      , date(time_unavailable) as end_date
      , round(duration_seconds/60) as duration
      , plays_count
      , plans
      , is_available
      , ARRAY_TO_STRING(JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres))), ",") as genres
      , array_length(JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))) as total_genres
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(0)] as genre_1
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(1)] as genre_2
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(2)] as genre_3
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(3)] as genre_4
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(4)] as genre_5
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(5)] as genre_6
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(6)] as genre_7
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(7)] as genre_8
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(8)] as genre_9
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(9)] as genre_10
      , JSON_VALUE_ARRAY(json_strip_nulls(parse_json(genres)))[safe_offset(10)] as genre_11
      from p0
      where n=1
      ;;
  }


  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension: media_type {
    type: string
    sql: ${TABLE}.media_type ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season_collection_name {
    type: string
    sql: ${TABLE}.season_collection_name ;;
  }

  dimension: movie_collection_name {
    type: string
    sql: ${TABLE}.movie_collection_name ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: string
    sql: ${TABLE}.episode ;;
  }

  dimension_group: date {
    group_label: "Published"
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension_group: end_date {
    group_label: "Published"
    type: time
    sql: ${TABLE}.end_date ;;
  }

  dimension_group: live {
    group_label: "Published"
    type: duration
    sql_start: ${TABLE}.date ;;
    sql_end: ${TABLE}.end_date ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension: is_available {
    type: yesno
    sql: ${TABLE}.is_available ;;
  }

  dimension: plans {
    type: string
    sql: ${TABLE}.plans ;;
  }

  dimension: plays_count {
    type: number
    sql: ${TABLE}.plays_count ;;
  }

  dimension: genres {
    group_label: "Genres"
    type: string
    sql: ${TABLE}.genres ;;
  }

  dimension: total_genres {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.total_genres ;;
  }

  dimension: primary_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_1 ;;
  }

  dimension: secondary_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_2 ;;
  }

  dimension: third_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_3 ;;
  }

  dimension: fourth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_4 ;;
  }

  dimension: fifth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_5 ;;
  }

  dimension: sixth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_6 ;;
  }

  dimension: seventh_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_7 ;;
  }

  dimension: eigth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_8 ;;
  }

  dimension: ninth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_9 ;;
  }

  dimension: tenth_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_10 ;;
  }

  dimension: eleventh_genre {
    group_label: "Genres"
    type: number
    sql: ${TABLE}.genre_11 ;;
  }

  set: detail {
    fields: [title, video_id, timestamp_time]
  }
}
