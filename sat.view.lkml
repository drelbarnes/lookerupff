view: sat {
  derived_table: {
    sql: WITH bigquery_timeupdate AS (((with a30 as
      (select video_id,
             max(ingest_at) as ingest_at
      from php.get_titles
      group by 1),

      a3 as
      (select distinct
             metadata_series_name as series,
              case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
                  when metadata_season_name is null then metadata_movie_name
                  when metadata_season_name is null and metadata_movie_name is null and a.duration_seconds>2700 then a.title
                  else metadata_season_name end as collection,
             season_number as season,
             a.title,
             a.video_id as id,
             episode_number as episode,
             date(time_available) as date,
             round(duration_seconds/60) as duration,
             promotion
      from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id inner join a30 on a30.video_id=a.video_id and a30.ingest_at=a.ingest_at
       where date(a.loaded_at)>='2020-02-13'  ),

      a31 as
      (select mysql_roku_firstplays_firstplay_date_date as timestamp,
                      mysql_roku_firstplays_video_id,
                      user_id,
                      max(loaded_at) as maxloaded
      from looker.roku_firstplays
      group by 1,2,3),

      a32 as
      (select a31.timestamp,
             a31.mysql_roku_firstplays_video_id,
             a31.user_id,
             count(*) as numcount,
             sum(mysql_roku_firstplays_total_minutes_watched) as mysql_roku_firstplays_total_minutes_watched
      from looker.roku_firstplays as a inner join a31 on a.loaded_at=maxloaded and mysql_roku_firstplays_firstplay_date_date=a31.timestamp and a31.mysql_roku_firstplays_video_id=a.mysql_roku_firstplays_video_id and a.user_id=a31.user_id
      group by 1,2,3),

      a4 as
      ((SELECT
          a3.title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(a3.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Web' AS source
        FROM
          javascript.durationchange as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

      union all

      (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'iOS' AS source
        FROM
          ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'iOS' AS source
        FROM
          ios.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Roku' AS source
        FROM
          roku.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Android' AS source
        FROM
          android.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          a3.title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(a3.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Web' AS source
        FROM
          javascript.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Roku' AS source
        FROM
          roku.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

      (SELECT
          title,
          a.user_id,
          email,
          video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Android' AS source
        FROM
          android.timeupdate as a inner join a3 on a.video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          distinct
          a3.title,
          a.user_id,
          email,
           mysql_roku_firstplays_video_id as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          a.timestamp,
          a3.duration*60 as duration,
          mysql_roku_firstplays_total_minutes_watched*60 as timecode,
         'Roku' AS source
        FROM
          a32 as a inner join a3 on  mysql_roku_firstplays_video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and a.user_id<>'0'*/ and a3.duration>0))

      select *,
             case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
                  else "NA"
                  end as Quarter
      from a4 as a))),

      sat0 as
      (SELECT
        bigquery_timeupdate.email  AS email,
        bigquery_timeupdate.user_id  AS user_id,
        bigquery_timeupdate.collection  AS collection,
        bigquery_timeupdate.title AS title,
        case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end as percent_completed,
        case when collection in ('Once Upon A Date','New Life','Barry Brewer: Chicago I\'m Home','All Good Things','The Furnace') and (case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end)>70 then 2
        when (case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end)>70 then 1 else 0 end  AS points
      FROM bigquery_timeupdate inner join (select distinct email from php.get_sat_users_backfill) as b on bigquery_timeupdate.email=b.email

      WHERE (collection like '%Heartland%' OR collection like '%Keeping Up with the Kaimanawas%' or collection like '%Morgan Family Strong%' or collection like '%Neon Rider%' or collection like '%Saddle Club%' or collection like '%Wild at Heart%' or (collection='Once Upon a Date' and date(bigquery_timeupdate.timestamp)='2020-04-17') or (collection='New Life' and date(bigquery_timeupdate.timestamp)='2020-04-18') or (collection='Barry Brewer: Chicago I\'m Home' AND date(bigquery_timeupdate.timestamp)='2020-04-19') or (collection='All Good Things' and date(bigquery_timeupdate.timestamp)='2020-04-21') or (collection='The Furnace' and date(bigquery_timeupdate.timestamp)='2020-04-23')) AND date(bigquery_timeupdate.timestamp)>='2020-04-17'
      GROUP BY 1,2,3,4)

      select email,
             user_id,
             sum(points) as points
      from sat0
      group by 1,2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: points {
    type: number
    sql: ${TABLE}.points ;;
  }

  set: detail {
    fields: [email, user_id, points]
  }
}
