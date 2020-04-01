view: derived_redshift_add_watchlist {
  derived_table: {
    sql: with a as
        (select distinct a.received_at,
                 a.user_id,
                 a.video_id,
                 b.title,
                 b.short_description,
                 b.thumbnail,
                 b.url,
                 'Android' as source
          from android.added_to_watch_list as a, php.get_titles as b WHERE a.video_id = b.video_id AND a.video_id NOT IN (SELECT video_id FROM android.removed_from_watch_list WHERE user_id = a.user_id))
        ,
          b as
         (select distinct a.received_at,
                 a.user_id,
                 a.video_id,
                 b.title,
                 b.short_description,
                 b.thumbnail,
                 b.url,
                 'iOS' as source
          from ios.added_to_watch_list as a, php.get_titles as b WHERE a.video_id = b.video_id AND a.video_id NOT IN (SELECT video_id FROM ios.removed_from_watch_list WHERE user_id = a.user_id))
        ,
        c as
         (select distinct a.received_at,
                 a.user_id,
                 a.video_id,
                 b.title,
                 b.short_description,
                 b.thumbnail,
                 b.url,
                 'Roku' as source
          from roku.added_to_watch_list as a, php.get_titles as b WHERE a.video_id = b.video_id AND a.video_id NOT IN (SELECT video_id FROM roku.removed_from_watch_list WHERE user_id = a.user_id))
          ,
          d as
         (select distinct a.received_at,
                 a.user_id,
                 b.video_id,
                 b.title,
                 b.short_description,
                 b.thumbnail,
                 split_part(a.context_page_url,'/', 4) AS url,
                 'Web' as source
          from javascript.added_to_watch_list as a, php.get_titles as b WHERE url = b.url AND url NOT IN (SELECT split_part(context_page_url,'/', 4) AS url FROM javascript.removed_from_watch_list WHERE user_id = a.user_id))


          (       select *
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

  dimension: short_description {
    type: string
    sql: ${TABLE}.short_description ;;
  }

  dimension: thumbnail {
    type: string
    sql: ${TABLE}.thumbnail ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
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
