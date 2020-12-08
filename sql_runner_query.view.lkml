view: video_content_playing {
  derived_table: {
    sql: with r as
      (
        SELECT date(received_at) as 'reporting_date', 'roku' as platfrom, count(distinct id) FROM roku.video_content_playing
      ),

      a as (

        SELECT date(received_at) as 'reporting_date', 'android' as platform, count(distinct id) FROM android.video_content_playing

      ),

      i as (

        SELECT date(received_at) as 'reporting_date', 'ios' as android, count(distinct id) FROM ios.video_content_playing

      ),

      w as

      (
        SELECT date(received_at) as 'reporting_date', 'web' as platform, count(distinct id) FROM javascript.video_content_playing
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

  dimension: platfrom {
    type: string
    sql: ${TABLE}.platfrom ;;
  }

  dimension: count_ {
    type: number
    sql: ${TABLE}.count ;;
  }

  set: detail {
    fields: [platfrom, count_]
  }
}
