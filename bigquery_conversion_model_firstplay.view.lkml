view: bigquery_conversion_model_firstplay {
  derived_table: {
    sql:
 WITH
      a AS (
(select distinct
       metadata_series_name as series,
       case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
            when metadata_season_name is null then metadata_movie_name else metadata_season_name end as collection,
       season_number as season,
       a.title,
       video_id as id,
       episode_number as episode,
       date(time_available) as date,
       round(duration_seconds/60) as duration,
       promotion
from php.get_titles as a inner join svod_titles.titles_id_mapping as b on a.video_id=b.id
where date(ingest_at)>='2020-02-13' )),

       web AS (
            SELECT
              user_id,
              'web' AS source,
              timestamp,
              case WHEN series LIKE '%Heartland%' THEN 'Heartland'
                WHEN series LIKE '%Bates%' THEN 'Bringing Up Bates'
                ELSE 'Other'
              END AS content,
              max(timecode) as duration
            FROM
              javascript.video_playback_started inner join a on video_playback_started.video_id=a.id
              group by 1,2,3,4),

            droid AS (
            SELECT
              user_id,
              'android' AS source,
              timestamp,
              case WHEN series LIKE '%Heartland%' THEN 'Heartland'
                WHEN series LIKE '%Bates%' THEN 'Bringing Up Bates'
                ELSE 'Other'
              END AS content,
              max(timecode) as duration
            FROM
              android.video_playback_started
            INNER JOIN
              a
            ON
              video_playback_started.video_id=a.id
              group by 1,2,3,4),

            roku AS (
            SELECT
             user_id,
              'roku' AS source,
              timestamp,
              case WHEN series LIKE '%Heartland%' THEN 'Heartland'
                WHEN series LIKE '%Bates%' THEN 'Bringing Up Bates'
                ELSE 'Other'
              END AS content,
              max(timecode) as duration
            FROM
              roku.video_playback_started
            INNER JOIN
              a
            ON
              video_playback_started.video_id=a.id
              group by 1,2,3,4),

            apple AS (
            SELECT
              user_id,
              'ios' AS source,
              timestamp,
              case WHEN series LIKE '%Heartland%' THEN 'Heartland'
                WHEN series LIKE '%Bates%' THEN 'Bringing Up Bates'
                ELSE 'Other'
              END AS content,
              max(timecode) as duration
            FROM
              ios.video_playback_started
            INNER JOIN
              a
            ON
              SAFE_CAST(video_playback_started.video_id AS int64)=SAFE_CAST(a.id AS int64)
              group by 1,2,3,4),

      b AS (
      SELECT * FROM web
      UNION ALL
      SELECT * FROM droid
      UNION ALL
      SELECT * FROM apple
      UNION ALL
      SELECT * FROM roku),

purchase_event as
(with
b as
(select user_id, min(received_at) as received_at
from http_api.purchase_event
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(received_at) and date(created_at)>'2018-10-31'
group by 1)

select a.user_id, a.platform, created_at
from b inner join http_api.purchase_event as a on a.user_id=b.user_id and a.received_at=b.received_at
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(a.received_at) and date(created_at)>'2018-10-31'),

    c as
    (SELECT
      a.user_id,
      platform,
      created_at,
    --   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
      sum(case when content = 'Heartland' then 1 else 0 end) as watched_heartland,
      sum(case when content = 'Bringing Up Bates'  then 1 else 0 end) as watched_bates,
      sum(case when content = 'Other' then 1 else 0 end) as watched_other,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_heartland_day_1,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_heartland_day_2,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_heartland_day_3,
      sum(case when content = 'Heartland' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_heartland_day_4,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_bates_day_1,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_bates_day_2,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_bates_day_3,
      sum(case when content = 'Bringing Up Bates' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_bates_day_4,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)<4 then 1 else 0 end) as watched_other_day_1,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=4 and date_diff(date(b.timestamp), date(created_at), day)<8 then 1 else 0 end) as watched_other_day_2,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=8 and date_diff(date(b.timestamp), date(created_at), day)<12 then 1 else 0 end) as watched_other_day_3,
      sum(case when content = 'Other' and date_diff(date(b.timestamp), date(created_at), day)>=12 and date_diff(date(b.timestamp), date(created_at), day)<16 then 1 else 0 end) as watched_other_day_4
    FROM
       purchase_event as a left join b ON a.user_id=b.user_id
    where date(b.timestamp)>=date(created_at) and date(b.timestamp)<=date_add(date(created_at), interval 14 day)
    group by 1,2,3),

    d as
    (select a.user_id,
           a.platform,
           a.created_at,
           case when watched_heartland is null then 0 else watched_heartland end as watched_heartland,
           case when watched_heartland_day_1 is null then 0 else watched_heartland_day_1 end as watched_heartland_day_1,
           case when watched_heartland_day_2 is null then 0 else watched_heartland_day_2 end as watched_heartland_day_2,
           case when watched_heartland_day_3 is null then 0 else watched_heartland_day_3 end as watched_heartland_day_3,
           case when watched_heartland_day_4 is null then 0 else watched_heartland_day_4 end as watched_heartland_day_4,
           case when watched_bates is null then 0 else watched_bates end as watched_bates,
           case when watched_bates_day_1 is null then 0 else watched_bates_day_1 end as watched_bates_day_1,
           case when watched_bates_day_2 is null then 0 else watched_bates_day_2 end as watched_bates_day_2,
           case when watched_bates_day_3 is null then 0 else watched_bates_day_3 end as watched_bates_day_3,
           case when watched_bates_day_4 is null then 0 else watched_bates_day_4 end as watched_bates_day_4,
           case when watched_other is null then 0 else watched_other end as watched_other,
           case when watched_other_day_1 is null then 0 else watched_other_day_1 end as watched_other_day_1,
           case when watched_other_day_2 is null then 0 else watched_other_day_2 end as watched_other_day_2,
           case when watched_other_day_3 is null then 0 else watched_other_day_3 end as watched_other_day_3,
           case when watched_other_day_4 is null then 0 else watched_other_day_4 end as watched_other_day_4
    from purchase_event as a left join c on a.user_id=c.user_id),

    web_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from javascript.video_playback_started as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    android_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from android.video_playback_started as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    ios_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from ios.video_playback_started  as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    roku_days as
    (select distinct a.user_id,
                    date(a.timestamp) as timestamp
    from roku.video_playback_started  as a right join purchase_event as b on a.user_id=b.user_id
    where date(a.timestamp)>=date(created_at) and date(a.timestamp)<=date_add(date(created_at), interval 14 day)),

    all_days as
    (select * from web_days
    union all
    select * from android_days
    union all
    select * from ios_days
    union all
    select * from roku_days),

    e as
    (select distinct user_id,
                     timestamp
     from all_days),

    f as
    (select user_id,
           count(*) as days_played
    from e
    group by 1)

    (select d.*,
           coalesce(days_played,0) as days_played
    from d left join f on d.user_id=f.user_id
    where d.user_id<>'0');;}

      dimension: user_id {
        primary_key: yes
        tags: ["user_id"]
        type: string
        sql: ${TABLE}.user_id ;;
      }

      dimension_group: customer_created_at {
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
        sql: ${TABLE}.customer_created_at ;;
      }

      dimension: days_played {
        type: number
        sql: ${TABLE}.days_played ;;
      }

      dimension: platform {
        type: string
        sql: ${TABLE}.platform ;;
      }


      dimension: heartland_play{
        type: number
        sql: ${TABLE}.watched_heartland ;;
      }

      dimension: heartland_play_day_1 {
        type: number
        sql: ${TABLE}.watched_heartland_day_1 ;;
      }

      dimension: heartland_play_day_2 {
        type: number
        sql: ${TABLE}.watched_heartland_day_2 ;;
      }

      dimension: heartland_play_day_3 {
        type: number
        sql: ${TABLE}.watched_heartland_day_3 ;;
      }

  dimension: heartland_play_day_4 {
    type: number
    sql: ${TABLE}.watched_heartland_day_4 ;;
  }

      dimension: bates_play{
        type: number
        sql: ${TABLE}.watched_bates ;;
      }

      dimension: bates_play_day_1 {
        type: number
        sql: ${TABLE}.watched_bates_day_1 ;;
      }

      dimension: bates_play_day_2 {
        type: number
        sql: ${TABLE}.watched_bates_day_2 ;;
      }

      dimension: bates_play_day_3 {
        type: number
        sql: ${TABLE}.watched_bates_day_3 ;;
      }

  dimension: bates_play_day_4 {
    type: number
    sql: ${TABLE}.watched_bates_day_4 ;;
  }

      dimension: other_play {
        type: number
        sql: ${TABLE}.watched_other ;;
      }

      dimension: other_play_day_1 {
        type: number
        sql: ${TABLE}.watched_other_day_1 ;;
      }

      dimension: other_play_day_2 {
        type: number
        sql: ${TABLE}.watched_other_day_2 ;;
      }

      dimension: other_play_day_3 {
        type: number
        sql: ${TABLE}.watched_other_day_3 ;;
      }

  dimension: other_play_day_4 {
    type: number
    sql: ${TABLE}.watched_other_day_4 ;;
  }

    }
