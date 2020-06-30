view: metadata_live_grid {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.customers.metadata_live_grid`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: live_date {
    type: date
    sql: ${TABLE}.Live_Date ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: end_date {
    type: date
    sql: ${TABLE}.End_Date ;;
  }

  dimension: asset_id {
    type: string
    sql: ${TABLE}.Asset_ID ;;
  }

  dimension: apple_id {
    type: string
    sql: ${TABLE}.Apple_ID ;;
  }

  dimension: original {
    type: string
    sql: ${TABLE}.Original ;;
  }

  dimension: exclusive {
    type: string
    sql: ${TABLE}.Exclusive ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.Type ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.Title ;;
  }

  dimension: episode_name {
    type: string
    sql: ${TABLE}.Episode_Name ;;
  }

  dimension: title_sort {
    type: string
    sql: ${TABLE}.Title_Sort ;;
  }

  dimension: title_brief {
    type: string
    sql: ${TABLE}.Title_Brief ;;
  }

  dimension: title_full {
    type: string
    sql: ${TABLE}.Title_Full ;;
  }

  dimension: rating {
    type: string
    sql: ${TABLE}.Rating ;;
  }

  dimension: hd_or_sd {
    type: string
    sql: ${TABLE}.HD_or_SD ;;
  }

  dimension: movies_tms {
    type: string
    sql: ${TABLE}.Movies_TMS ;;
  }

  dimension: series_tms {
    type: string
    sql: ${TABLE}.Series_TMS ;;
  }

  dimension: episode_tms {
    type: string
    sql: ${TABLE}.Episode_TMS ;;
  }

  dimension: eidr_id {
    type: string
    sql: ${TABLE}.EIDR_ID ;;
  }

  dimension: imdb_id {
    type: string
    sql: ${TABLE}.IMDB_ID ;;
  }

  dimension: studio {
    type: string
    sql: ${TABLE}.Studio ;;
  }

  dimension: trt {
    type: string
    sql: ${TABLE}.TRT ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.Year ;;
  }

  dimension: original_air_date {
    type: string
    sql: ${TABLE}.Original_Air_Date ;;
  }

  dimension: primary_genre {
    type: string
    sql: ${TABLE}.Primary_Genre ;;
  }

  dimension: secondary_genre {
    type: string
    sql: ${TABLE}.Secondary_Genre ;;
  }

  dimension: actors {
    type: string
    sql: ${TABLE}.Actors ;;
  }

  dimension: director {
    type: string
    sql: ${TABLE}.Director ;;
  }

  dimension: summary__100_ {
    type: string
    sql: ${TABLE}.Summary__100_ ;;
  }

  dimension: summary__256_ {
    type: string
    sql: ${TABLE}.Summary__256_ ;;
  }

  dimension: summary {
    type: string
    sql: ${TABLE}.Summary ;;
  }

  set: detail {
    fields: [
      live_date,
      end_date,
      asset_id,
      apple_id,
      original,
      exclusive,
      type,
      title,
      episode_name,
      title_sort,
      title_brief,
      title_full,
      rating,
      hd_or_sd,
      movies_tms,
      series_tms,
      episode_tms,
      eidr_id,
      imdb_id,
      studio,
      trt,
      year,
      original_air_date,
      primary_genre,
      secondary_genre,
      actors,
      director,
      summary__100_,
      summary__256_,
      summary
    ]
  }
}
