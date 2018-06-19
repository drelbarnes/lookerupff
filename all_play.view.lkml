view: all_play {
  derived_table: {
    sql: select a.timestamp,trim(upper(title)) as title,user_id,'Android' as platform from android.play as a left join svod_titles.title_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,trim(upper(title)) as title,user_id,'IOS' as platform from ios.play as a left join svod_titles.title_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,trim(upper(split_part(title,'-',1))) as title,user_id,'Web' as platform from javascript.play as a ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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
