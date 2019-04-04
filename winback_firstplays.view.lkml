view: winback_firstplays {
  derived_table: {
    sql: with aa as
      (select user_id,status_date as churn_date
      from http_api.purchase_event
      where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

      bb as
      (select user_id, max(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
      group by 1),

      cc as
      (select distinct bb.user_id
      from aa inner join bb on aa.user_id=bb.user_id and status_date>churn_date),

      a1 as
      (select sent_at as timestamp,
              user_id,
              (split(title," - ")) as title
      from javascript.firstplay),

      a2 as
      (select timestamp,
              user_id,
              title[safe_ordinal(1)] as title,
              concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
       from a1 order by 1),

      titles_id_mapping as
      (select *
      from svod_titles.titles_id_mapping
      where collection not in ('Romance - OLD',
      'Dramas',
      'Comedies',
      'Kids - OLD',
      'Christmas',
      'Just Added',
      'Music',
      'Faith Movies',
      'Docs & Specials',
      'Trending',
      'Adventure',
      'All Movies',
      'All Series',
      'Bonus Content',
      'Drama Movies',
      'Drama Series',
      'Faith Favorites',
      'Family Addition',
      'Family Comedies',
      'Fan Favorite Series',
      'Fantasy',
      'Kids',
      'New',
      'New Series',
      'Romance',
      'Sports',
      'The Must-Watch List',
      'UPlifting Reality',
      'UP Original Movies and Series',
      'UP Original Series'
      )),

      a32 as
      (select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                      mysql_roku_firstplays_video_id,
                      user_id
      from looker.roku_firstplays),

      a as
              (select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim((title)) as title,
                      user_id,
                      'Android' as source,
                      episode
               from android.firstplay as a left join titles_id_mapping as b on a.video_id = b.id

               union all

              select timestamp,
             b.date,
             case when b.collection in ('Season 1','Season 2','Season 3') then concat(b.series,' ',b.collection) else collection end as collection,
             case when b.series is null and upper(b.collection)=upper(b.title) then 'movie'
                           when b.series is not null then 'series' else 'other' end as type,
             mysql_roku_firstplays_video_id as video_id,
             trim(b.title) as title,
             user_id,
             'Roku' as source,
             b.episode
      from a32 as a left join titles_id_mapping as b on mysql_roku_firstplays_video_id=b.id


               union all

               select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim((title)) as title,
                      user_id,
                      'iOS' as source,
                      episode
               from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
               union all
               select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim((title)) as title,
                      user_id,
                      'Web' as source,
                      episode
               from javascript.loadeddata as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)
               union all
               select timestamp,
                      b.date as release_date,
                      case when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection) else b.collection end as collection,
                      case when series is null and upper(b.collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(b.id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      'Web' as source,
                      episode
               from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))


      select a.user_id,
             a.timestamp,
             a.release_date,
             a.collection,
             a.type,
             a.video_id,
             a.title,
             a.source,
             a.episode,
             case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
                  else "NA"
                  end as Quarter
      from a inner join cc on a.user_id=cc.user_id
      where a.user_id<>'0'
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: release_date {
    type: date
    sql: ${TABLE}.release_date ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: episode {
    type: number
    sql: ${TABLE}.episode ;;
  }

  dimension: quarter {
    type: string
    sql: ${TABLE}.Quarter ;;
  }

  set: detail {
    fields: [
      user_id,
      timestamp_time,
      release_date,
      collection,
      type,
      video_id,
      title,
      source,
      episode,
      quarter
    ]
  }
}
