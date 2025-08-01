view: active_users {
  derived_table: {
    sql: with allfirstplay as
(
--begin plays.cta1

with a1 as
(
--allfirstplay.cta0
select
  sent_at as timestamp,
  user_id,
  (split(title," - ")) as title
from javascript.firstplay
where date(sent_at) > '2025-06-01'
),

a2 as
(
--allfirstplay.cta1
select
  timestamp,
  user_id,
  title[safe_ordinal(1)] as title,
  concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
from a1
order by 1
),

titles_id_mapping as
(
--allfirstplay.cta2
select
  *
from svod_titles.titles_id_mapping
where collection not in (
'Romance - OLD',
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
'UP Original Series')
),

a as
--allfirstplay.cta3
(
select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((title)) as title,
  user_id,
  'Android' as source,
  episode
from android.firstplay as a
left join titles_id_mapping as b
on a.video_id = b.id
where date(sent_at) > '2025-06-01'


union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((title)) as title,
  user_id,
  'iOS' as source,
  episode
from ios.firstplay as a
left join titles_id_mapping as b
on a.video_id = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'


union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((title)) as title,
  user_id,
  'Roku' as source,
  episode
from roku.firstplay as a
left join titles_id_mapping as b
on a.video_id = b.id
where date(sent_at) > '2025-06-01'


union all

select
  timestamp,
  b.date as release_date,
  b.collection,
  case
    when series is null and upper(b.collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(b.id as int64) as video_id,
  trim(b.title) as title,
  user_id,
  'Web' as source,
  episode
from a2 as a
left join titles_id_mapping as b
on trim(upper(b.title)) = trim(upper(a.title))
where date(timestamp) > '2025-06-01'


union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'Web' as source,
  episode
from javascript.loadedmetadata as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'

union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'Web' as source,
  episode
from javascript.video_content_playing as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'


union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'iOS' as source,
  episode
from ios.video_content_playing as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'

union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'Android' as source,
  episode
from android.video_content_playing as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'

union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'FireTV' as source,
  episode
from amazon_fire_tv.video_content_playing as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'

union all

select
  sent_at as timestamp,
  b.date as release_date,
  collection,
  case
    when series is null and upper(collection)=upper(b.title) then 'movie'
    when series is not null then 'series' else 'other' end as type,
  safe_cast(a.video_id as int64) as video_id,
  trim((b.title)) as title,
  user_id,
  'Roku' as source,
  episode
from roku.video_content_playing as a
left join titles_id_mapping as b
on safe_cast(a.video_id as string) = safe_cast(b.id as string)
where date(sent_at) > '2025-06-01'

)

select
  *,
  case
    when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
    when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
    when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
    else "NA"
  end as Quarter
from a
where user_id <> '0'
),

audience as
(
--begin plays.cta2
select
  FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CAST(timestamp  AS TIMESTAMP), DAY), INTERVAL (0 - CAST((CASE WHEN (EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7 < 0 THEN -1 * (ABS((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) / (7))) AS INT64))) ELSE ABS((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) - (ABS(7) * CAST(FLOOR(ABS(((EXTRACT(DAYOFWEEK FROM timestamp ) - 1) - 1 + 7) / (7))) AS INT64)) END) AS INT64)) DAY), DAY)) as week,
  user_id
from allfirstplay

)
select distinct * from audience where week = FORMAT_TIMESTAMP('%F', TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), WEEK(MONDAY)), INTERVAL 7 DAY))
 ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: week {
    type: date
    sql: ${TABLE}.week ;;
  }
}
