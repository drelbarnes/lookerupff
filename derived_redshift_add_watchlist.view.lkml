view: derived_redshift_add_watchlist {
  derived_table: {
    sql: with a as
        (select a.received_at,
                a.user_id,
                a.video_id,
                'Android' as source
         from android.added_to_watch_list as a left join android.users as b on a.id = b.id AND a.user_id != cast(0 as string))
        ,
          b as
         (select a.received_at,
                a.user_id,
                a.video_id,
                'iOS' as source
         from ios.added_to_watch_list as a left join ios.users as b on a.user_id = b.id AND a.user_id != cast(0 as string))
        ,
        c as
         (select a.received_at,
                a.user_id,
                a.video_id,
                'Roku' as source
         from roku.added_to_watch_list as a left join roku.users as b on a.user_id = b.id AND a.user_id != cast(0 as string))


          (       select *
                  from a
                  union all
                  select * from b
                  union all
                  select * from c

          );;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.email ;;
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

  measure: count {
    type: count
  }

  measure: discount_count {
    type: count_distinct
    sql: ${user_id};;
  }



}
