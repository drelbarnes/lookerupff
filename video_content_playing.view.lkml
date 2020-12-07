view: video_content_playing {
  derived_table: {
    sql: with r as
      (
        SELECT 'roku' as platfrom, count(distinct id) FROM roku.video_content_playing WHERE date(received_at) between '2020-11-01' and '2020-11-30'
      ),

      a as (

        SELECT 'android' as platform, count(distinct id) FROM android.video_content_playing WHERE date(received_at) between '2020-11-01' and '2020-11-30'

      ),

      i as (

        SELECT 'ios' as android, count(distinct id) FROM ios.video_content_playing WHERE date(received_at) between '2020-11-01' and '2020-11-30'

      ),

      w as

      (
        SELECT 'web' as platform, count(distinct id) FROM javascript.video_content_playing WHERE date(received_at) between '2020-11-01' and '2020-11-30'
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
