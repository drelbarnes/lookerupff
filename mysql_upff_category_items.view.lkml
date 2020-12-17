view: mysql_upff_category_items {

  sql_table_name: admin_roku.upff_library_category_items ;;
  drill_fields: [id]

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: number
    primary_key: yes
    tags: ["user_id"]
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: item_id {
    type: string
    sql: ${TABLE}.item_id ;;
  }

  dimension: cat_id {
    type: string
    sql: ${TABLE}.cat_id ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: created_at {
    type: string
    sql: ${TABLE}.created_at ;;
  }

  dimension: updated_at {
    type: string
    sql: ${TABLE}.updated_at ;;
  }

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension: season_count {
    type: number
    sql: ${TABLE}.season_count ;;
  }

  dimension: items_count {
    type: number
    sql: ${TABLE}.items_count ;;
  }

  dimension: has_free_videos {
    type: number
    sql: ${TABLE}.has_free_videos ;;
  }

  dimension: primary_genre {
    type: string
    sql: ${TABLE}.primary_genre ;;
  }

  dimension: secondary_genre {
    type: string
    sql: ${TABLE}.secondary_genre ;;
  }

  dimension: cast {
    type: string
    sql: ${TABLE}.cast ;;
  }

  dimension: studio {
    type: string
    sql: ${TABLE}.studio ;;
  }

  dimension: released_year {
    type: string
    sql: ${TABLE}.released_year ;;
  }

  dimension: is_available {
    type: number
    sql: ${TABLE}.is_available ;;
  }

  dimension: is_featured {
    type: string
    sql: ${TABLE}.is_featured ;;
  }

  dimension: seconds {
    type: string
    sql: ${TABLE}.seconds ;;
  }

  dimension: formatted {
    type: string
    sql: ${TABLE}.formatted ;;
  }

  dimension: is_automatic {
    type: number
    sql: ${TABLE}.is_automatic ;;
  }

  dimension: cat_order {
    type: number
    sql: ${TABLE}.cat_order ;;
  }

  dimension: item_order {
    type: number
    sql: ${TABLE}.item_order ;;
  }

  dimension: slug {
    type: string
    sql: ${TABLE}.slug ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
  }

  dimension_group: ingested_at {
    type: time
    sql: ${TABLE}.ingested_at ;;
  }

  set: detail {
    fields: [
      id,
      name,
      item_id,
      cat_id,
      type,
      created_at,
      updated_at,
      short_description,
      season_count,
      items_count,
      has_free_videos,
      primary_genre,
      secondary_genre,
      cast,
      studio,
      released_year,
      is_available,
      is_featured,
      seconds,
      formatted,
      is_automatic,
      cat_order,
      item_order,
      slug,
      thumbnail,
      ingested_at_time
    ]
  }
}
