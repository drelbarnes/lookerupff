view: all_play {
  derived_table: {
    sql: (with a as
        (select a.timestamp,trim(upper(split_part(series,'|',1))) as series,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(title)) as title,
                user_id,
                'Android' as platform
         from android.play as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,
                trim(upper(split_part(series,'|',1))) as series,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(title)) as title,
                user_id,
                'IOS' as platform
         from ios.play as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,
                trim(upper(split_part(series,'|',1))) as series,
                cast(b.id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(split_part(a.title,'-',1))) as title,
                user_id,
                'Web' as platform
         from javascript.play as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title))=trim(upper(split_part(a.title,'-',1))) )

select a.*, status
from a inner join customers.customers on user_id=customer_id) ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: string
    sql: ${TABLE}.episode ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }



# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      title,
      platform,
      user_id
    ]
  }
}
