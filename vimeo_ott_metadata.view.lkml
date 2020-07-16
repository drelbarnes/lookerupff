view: vimeo_ott_metadata {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.customers.vimeo_ott_metadata`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: vimeo_ott_id {
    type: number
    sql: ${TABLE}.Vimeo_OTT_ID ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1;;
  }

  dimension: content_type {
    type: string
    sql: ${TABLE}.Content_Type ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.Title ;;
  }

  dimension: series_id {
    type: string
    sql: ${TABLE}.Series_ID ;;
  }

  dimension: season_id {
    type: string
    sql: ${TABLE}.Season_ID ;;
  }

  dimension: movie_id {
    type: string
    sql: ${TABLE}.Movie_ID ;;
  }

  dimension: category_id {
    type: string
    sql: ${TABLE}.Category_ID ;;
  }

  dimension: playlist_id {
    type: string
    sql: ${TABLE}.Playlist_ID ;;
  }

  dimension: sequence_number {
    type: string
    sql: ${TABLE}.Sequence_Number ;;
  }

  dimension: short_description {
    type: string
    sql: ${TABLE}.Short_Description ;;
  }

  dimension: long_description {
    type: string
    sql: ${TABLE}.Long_Description ;;
  }

  dimension: availability__geography_ {
    type: string
    sql: ${TABLE}.Availability__Geography_ ;;
  }

  dimension: unavailability__geography_ {
    type: string
    sql: ${TABLE}.Unavailability__Geography_ ;;
  }

  dimension: availability__start_time_ {
    type: string
    sql: ${TABLE}.Availability__Start_Time_ ;;
  }

  dimension: availability__end_time_ {
    type: string
    sql: ${TABLE}.Availability__End_Time_ ;;
  }

  dimension: plans {
    type: string
    sql: ${TABLE}.Plans ;;
  }

  dimension: drm_enabled_ {
    type: string
    sql: ${TABLE}.DRM_Enabled_ ;;
  }

  dimension: media_type {
    type: string
    sql: ${TABLE}.Media_Type ;;
  }

  dimension: movie_type {
    type: string
    sql: ${TABLE}.Movie_Type ;;
  }

  dimension: movie_version {
    type: string
    sql: ${TABLE}.Movie_Version ;;
  }

  dimension: sports_type {
    type: string
    sql: ${TABLE}.Sports_Type ;;
  }

  dimension: license {
    type: string
    sql: ${TABLE}.License ;;
  }

  dimension: production_id {
    type: string
    sql: ${TABLE}.Production_ID ;;
  }

  dimension: tags {
    type: string
    sql: ${TABLE}.Tags ;;
  }

  dimension: genres {
    type: string
    sql: ${TABLE}.Genres ;;
  }

  dimension: casts {
    type: string
    sql: ${TABLE}.Casts ;;
  }

  dimension: crew {
    type: string
    sql: ${TABLE}.Crew ;;
  }

  dimension: release_dates {
    type: string
    sql: ${TABLE}.Release_Dates ;;
  }

  dimension: ratings {
    type: string
    sql: ${TABLE}.Ratings ;;
  }

  dimension: advisories {
    type: string
    sql: ${TABLE}.Advisories ;;
  }

  dimension: production_studios {
    type: string
    sql: ${TABLE}.Production_Studios ;;
  }

  dimension: metadata_array__cast {
    type: string
    sql: ${TABLE}.Metadata_Array__cast ;;
  }

  dimension: metadata_integer__year_released {
    type: string
    sql: ${TABLE}.Metadata_Integer__year_released ;;
  }

  dimension: metadata_string__director {
    type: string
    sql: ${TABLE}.Metadata_String__director ;;
  }

  dimension: advertising__keywords {
    type: string
    sql: ${TABLE}.Advertising__keywords ;;
  }

  set: detail {
    fields: [
      vimeo_ott_id,
      content_type,
      title,
      series_id,
      season_id,
      movie_id,
      category_id,
      playlist_id,
      sequence_number,
      short_description,
      long_description,
      availability__geography_,
      unavailability__geography_,
      availability__start_time_,
      availability__end_time_,
      plans,
      drm_enabled_,
      media_type,
      movie_type,
      movie_version,
      sports_type,
      license,
      production_id,
      tags,
      genres,
      casts,
      crew,
      release_dates,
      ratings,
      advisories,
      production_studios,
      metadata_array__cast,
      metadata_integer__year_released,
      metadata_string__director,
      advertising__keywords
    ]
  }
}
