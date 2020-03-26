view: derived_redshift_add_watchlist {
  derived_table: {
    sql: with a as
        (select a.received_at,
                a.user_id,
                a.video_id,
                b.title,
                'Android' as source
         from android.added_to_watch_list as a left join php.get_titles as b  on a.video_id = b.video_id left join android.users as c on a.user_id = c.id )
        ,
          b as
         (select a.received_at,
                a.user_id,
                a.video_id,
                b.title,
                'iOS' as source
         from ios.added_to_watch_list as a left join php.get_titles as b  on a.video_id = b.video_id left join ios.users as c on a.user_id = c.id )
        ,
        c as
         (select a.received_at,
                a.user_id,
                a.video_id,
                b.title,
                'Roku' as source
         from roku.added_to_watch_list as a left join php.get_titles as b  on a.video_id = b.video_id left join roku.users as c on a.user_id = c.id )
        ,
         d as
         (select a.received_at,
                a.user_id,
                b.video_id,
                b.title,
                'Web' as source
         from javascript.added_to_watch_list as a left join php.get_titles as b  on REPLACE(a.context_page_path, '/', '') = b.url left join javascript.users as c on a.user_id = c.id )

          (       select distinct *
                  from a
                  union all
                  select * from b
                  union all
                  select * from c
                   union all
                  select * from d
          );;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }


  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
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
