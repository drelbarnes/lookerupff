view: video_content_playing_by_source {
  derived_table: {
    sql: with r as
      (
        SELECT id, received_at, 'roku' as platfrom, count(distinct id) FROM roku.video_content_playing
      ),

      a as (

        SELECT id, received_at, 'android' as platform, count(distinct id) FROM android.video_content_playing

      ),

      i as (

        SELECT id, received_at, 'ios' as android, count(distinct id) FROM ios.video_content_playing

      ),

      w as

      (
        SELECT id, received_at, 'web' as platform, count(distinct id) FROM javascript.video_content_playing
      ),

      b as (
      select * from r
      UNION ALL
      select * from a
      UNION ALL
      select * from i
      UNION ALL
      select * from w
      )

      select * from b
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: received_at {
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

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: platfrom {
    type: string
    sql: ${TABLE}.platfrom ;;
  }

  dimension: total_count {
    type: number
    sql: ${TABLE}.count ;;
  }

  set: detail {
    fields: [platfrom, total_count]
  }
}
