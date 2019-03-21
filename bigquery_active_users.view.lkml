view: bigquery_active_users {
  derived_table: {
    sql: with plays as
(with a0 as
(select date(analytics_timestamp) as timestamp,
       date_sub(date(analytics_timestamp),interval 7 day) week_ago,
       existing_free_trials+existing_paying as total_subs
from php.get_analytics
where date(sent_at)=current_date()
order by 1 desc),

allfirstplay as
(with a1 as
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

a as
        (select sent_at as timestamp,
                b.date as release_date,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                trim((title)) as title,
                user_id,
                'Android' as source,
                episode
         from android.firstplay as a left join titles_id_mapping as b on a.video_id = b.id
         union all
         select sent_at as timestamp,
                b.date as release_date,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                trim((title)) as title,
                user_id,
                'iOS' as source,
                episode
         from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
         union all
         select timestamp,
                b.date as release_date,
                b.collection,
                case when series is null and upper(b.collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(b.id as int64) as video_id,
                trim(b.title) as title,
                user_id,
                'Web' as source,
                episode
         from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))


select *,
       case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
            else "NA"
            end as Quarter
from a
where user_id<>'0'),

audience as
(select FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CAST(timestamp  AS TIMESTAMP), DAY), INTERVAL (0 - CAST((CASE WHEN (EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7 < 0 THEN -1 * (ABS((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) / (7))) AS INT64))) ELSE ABS((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) / (7))) AS INT64)) END) AS INT64)) DAY), DAY)) as week,
       count(distinct user_id) as audience_size
from allfirstplay
group by 1)

select cast(week_ago as timestamp) as timestamp,
       audience_size,
       total_subs
from audience inner join a0 on week=cast(week_ago as string)
order by 1 desc),

a32 as
(select distinct mysql_roku_firstplays_firstplay_date_date,
                mysql_roku_firstplays_video_id,
                user_id
from looker.roku_firstplays),

roku_plays as
(select FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CAST(mysql_roku_firstplays_firstplay_date_date  AS TIMESTAMP), DAY), INTERVAL (0 - CAST((CASE WHEN (EXTRACT(DAYOFWEEK FROM mysql_roku_firstplays_firstplay_date_date ) - 1) - 1 + 7 < 0 THEN -1 * (ABS((EXTRACT(DAYOFWEEK FROM mysql_roku_firstplays_firstplay_date_date ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM mysql_roku_firstplays_firstplay_date_date ) - 1) - 1 + 7) / (7))) AS INT64))) ELSE ABS((EXTRACT(DAYOFWEEK FROM mysql_roku_firstplays_firstplay_date_date ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM mysql_roku_firstplays_firstplay_date_date ) - 1) - 1 + 7) / (7))) AS INT64)) END) AS INT64)) DAY), DAY)) as timestamp,
       count(distinct user_id) as audience_size
from a32
where date(mysql_roku_firstplays_firstplay_date_date)>='2019-03-04'
group by 1)

select a.timestamp,
       a.audience_size+ifnull(b.audience_size,0) as audience_size,
       total_subs
from plays as a left join roku_plays as b on a.timestamp=cast(b.timestamp as timestamp)
       ;;
  }

  measure: count {
    type: count
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  measure: audience_size {
    type: sum
    sql: ${TABLE}.audience_size ;;
  }

  measure: total_subs {
    type: sum
    sql: ${TABLE}.total_subs ;;
  }

}
