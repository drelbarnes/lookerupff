view: redshift_php_mybundle_library {
  derived_table: {
    sql: SELECT distinct i.short_description, t.description, t.duration_seconds, t.metadata_year_released, i.thumbnail, t.additional_images_aspect_ratio_1_1_source,t.metadata_secondary_genre, t.metadata_primary_genre, t.title, t.url,t.metadata_movie_name, t.metadata_series_name FROM php.get_title_category_items i INNER JOIN php.get_titles as t ON i.slug = t.url WHERE t.is_available = 'true'
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: duration_seconds {
    type: number
    sql: ${TABLE}.duration_seconds ;;
  }

  dimension: metadata_year_released {
    type: number
    sql: CAST(${TABLE}.metadata_year_released AS CHAR) ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
  }

  dimension: additional_images_aspect_ratio_1_1_source {
    type:  string
    sql: ${TABLE}.additional_images_aspect_ratio_1_1_source ;;
  }

  dimension: metadata_secondary_genre {
    type: string
    sql: ${TABLE}.metadata_secondary_genre ;;
  }

  dimension: metadata_primary_genre {
    type: string
    sql: ${TABLE}.metadata_primary_genre ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: metadata_movie_name {
    type: string
    sql: ${TABLE}.metadata_movie_name ;;
  }

  dimension: metadata_series_name {
    type: string
    sql: ${TABLE}.metadata_series_name ;;
  }

  set: detail {
    fields: [
      short_description,
      description,
      duration_seconds,
      metadata_year_released,
      thumbnail,
      metadata_secondary_genre,
      metadata_primary_genre,
      title,
      url,
      metadata_movie_name,
      metadata_series_name
    ]
  }
}
