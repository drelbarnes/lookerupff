view: bigquery_titles {
  derived_table: {
    sql: with a as
          (select month,
             year,
             case when platform = 'Comcast SVOD' then 'Comcast' else platform end as platform,
             case when platform not in ('Amazon','Vimeo','Comcast SVOD') then 'All Others' else platform
                  end as platform_,
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
       from svod_titles.titles)

      select *, case when platform_ = 'Comcast SVOD' then 'Comcast' else platform_ end as platform__ from a

      ;;
  }


  dimension: platform_ {
    type: string
    sql: ${TABLE}.platform__;;
  }

  dimension: heartland_bates {
    type: string
    sql: case when ${franchise} like '%Heartland%' then 'Heartland'
              when ${franchise} like '%Bates%' then 'Bates'
         else 'Other' end;;
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
    sql: timestamp(${TABLE}.datetime);;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      month,
      quarter,
      year
    ]
    sql: timestamp(${TABLE}.datetime) ;;}

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

  measure: total_views_ {
    type: sum
    sql: ${views} ;;
  }

  measure: total_views {
    type: number
    sql: case when ${total_views_} is null then 0 else ${total_views_} end ;;
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
