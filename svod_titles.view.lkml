view: titles {
  derived_table: {
    sql: select month,
       year,
       case when platform = 'Comcast SVOD' then 'Comcast' else platform end as platform,
       up_title,
       studio,
       views,
       type,
       category,
       franchise,
       season,
       lf_sf,
       content_type,
       datetime
 from svod_titles.svod_titles ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: content_type {
    type: string
    sql: ${TABLE}.content_type ;;
  }

  dimension: franchise {
    type: string
    sql: ${TABLE}.franchise ;;
  }

  dimension: lf_sf {
    type: string
    sql: ${TABLE}.lf_sf ;;
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.datetime;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.datetime ;;}

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: season {
    type: number
    sql: ${TABLE}.season ;;
  }

  dimension: studio {
    type: string
    sql: ${TABLE}.studio ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: up_title {
    type: string
    sql: ${TABLE}.up_title ;;
  }

  dimension: views {
    type: number
    sql: ${TABLE}.views ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: total_views {
    type: sum
    sql: ${views} ;;
  }


  measure: episode_count {
    type: count_distinct
    sql: ${up_title} ;;
  }

  measure: avg_views_per_episode {
    type: number
    sql: ${total_views}/${episode_count} ;;
    value_format_name: decimal_0
  }
}
