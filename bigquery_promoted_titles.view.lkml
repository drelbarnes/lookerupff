view: bigquery_promoted_titles {
  derived_table: {
    sql: WITH bigquery_allfirstplay AS (with aa as
      (select user_id,email,status_date as churn_date
      from http_api.purchase_event
      where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

      bb as
      (select user_id, email, max(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
      group by 1,2),

      cc as
      (select distinct bb.user_id, bb.email
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
      'UP Original Series',
      'Best. Moms. Ever. | #CallYourMom'
      )),

      a32 as
      (select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                      mysql_roku_firstplays_video_id,
                      user_id,
                      '' as anonymousId,
                      'firstplay' as event_type,
                      UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
                      CAST('1111' AS int64) as platform_id
      from looker.roku_firstplays),

      a as
              (select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      anonymous_id,
                      event as event_type,
                      'Android' as source,
                      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                      CAST(platform_id AS int64) as platform_id,
                      episode,
                      cast(is_chromecast as int64) as tv_cast,
                      promotion
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
            'anonymous_id' as anonymous_id,
             'firstplay' as event_type,
            'Roku' as source,
            UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
            CAST('1111' AS int64) as platform_id,
             b.episode,
             null as tv_cast,
             promotion
      from a32 as a left join titles_id_mapping as b on mysql_roku_firstplays_video_id=b.id


               union all

               select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      anonymous_id,
                      event as event_type,
                      'iOS' as source,
                      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                      CAST(platform_id AS int64) as platform_id,
                      episode,
                      cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                      promotion
               from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
               union all
               select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      null as anonymous_id,
                      event as event_type,
                      'Roku' as source,
                      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                      CAST(platform_id AS int64) as platform_id,
                      episode,
                      cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                      promotion
               from roku.firstplay as a left join titles_id_mapping as b on a.video_id = b.id
               union all
               select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      anonymous_id,
                      'firstplay' as event_type,
                      'Web' as source,
                      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                      CAST(platform_id AS int64) as platform_id,
                      episode,
                      null as tv_cast,
                      promotion
               from javascript.loadedmetadata as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)
              union all
              select sent_at as timestamp,
                      b.date as release_date,
                      case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(a.video_id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      anonymous_id,
                      'firstplay' as event_type,
                      'Web' as source,
                      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                      CAST(platform_id AS int64) as platform_id,
                      episode,
                      null as tv_cast,
                      promotion
               from javascript.video_content_loaded as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

              union all

              select timestamp,
                      b.date as release_date,
                      case when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection) else b.collection end as collection,
                      case when series is null and upper(b.collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      safe_cast(b.id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      '' as anonymous_id,
                      'firstplay' as event_type,
                      'Web' as source,
                      UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
                      CAST('33064' AS int64) as platform_id,
                      episode,
                      null as tv_cast,
                      promotion
               from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))


      select a.user_id,
             a.anonymous_id,
             a.event_type,
             a.timestamp,
             a.EPOCH_TIMESTAMP,
             a.platform_id,
             a.release_date,
             a.collection,
             a.type,
             a.video_id,
             a.title,
             a.source,
             a.episode,
            email,
            tv_cast,
            promotion,
             case when cc.user_id is null then 'first-time customers' else 'reacquisitions' end as winback,
             case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
                  else "NA"
                  end as Quarter
      from a left join cc on a.user_id=cc.user_id
      where a.user_id<>'0'      )
      SELECT
        FORMAT_TIMESTAMP('%A', bigquery_allfirstplay.timestamp ) AS bigquery_allfirstplay_timestamp_day_of_week,
        COUNT(DISTINCT concat(safe_cast(bigquery_allfirstplay.video_id as string),bigquery_allfirstplay.user_id,cast((CAST(bigquery_allfirstplay.timestamp  AS DATE)) as string)) ) AS bigquery_allfirstplay_play_count,
        count(distinct collection) as title_count
      FROM bigquery_allfirstplay

      WHERE (bigquery_allfirstplay.promotion  IS NOT NULL) AND (((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.promotion  AS TIMESTAMP), DAY)) <= (TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY))) AND (TIMESTAMP_ADD((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.promotion  AS TIMESTAMP), DAY)), INTERVAL 7 DAY) > (TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY))))
      GROUP BY 1
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: timestamp_day_of_week {
    type: string
    sql: ${TABLE}.bigquery_allfirstplay_timestamp_day_of_week ;;
  }

  dimension: play_count {
    type: number
    sql: ${TABLE}.bigquery_allfirstplay_play_count ;;
  }

  measure: play_count_ {
    type: average
    sql: ${TABLE}.bigquery_allfirstplay_play_count ;;
  }

  measure: title_count {
    type: average
    sql: ${TABLE}.title_count ;;
  }

  set: detail {
    fields: [timestamp_day_of_week,play_count]
  }
}
