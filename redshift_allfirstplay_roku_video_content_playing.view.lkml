view: redshift_allfirstplay_roku_video_content_playing {
  derived_table: {
    sql:

with

a1 as
(
select
  sent_at as timestamp,
  user_id,
  split_part(title, ' - ', 1) as title
from javascript.firstplay
),

a2 as
(
select
  timestamp as timestamp,
  user_id,
  split_part(title, ' - ', 1) as title,
  split_part(title, ' - ', 2) || ' - ' || split_part(title, ' - ', 3) as collection
from a1
order by 1
),

a30 as
(
select
  video_id,
  max(ingest_at) as loaded_at
from php.get_titles
group by 1
),

titles_id_mapping as
(
select distinct
  metadata_series_name  as series,
  case
    when metadata_season_name in ('Season 1', 'Season 2', 'Season 3') then metadata_series_name || '-' || metadata_season_name
    when metadata_season_name is null then metadata_movie_name
    else metadata_season_name
  end as collection,
  metadata_season_number as season,
  a.title,
  a.video_id as id,
  episode_number as episode,
  date(time_available) as date,
  date(time_unavailable) as end_date,
  round(duration_seconds / 60) as duration
from php.get_titles as a
left join svod_titles.titles_id_mapping as b
on a.video_id = b.id
inner join a30
on a30.video_id = a.video_id
and a30.loaded_at = a.ingest_at
),

a32 as
(
select distinct
  mysql_roku_firstplays_firstplay_date_date as timestamp,
  mysql_roku_firstplays_video_id,
  user_id,
  '' as anonymousid,
  'firstplay' as event_type,
  extract(epoch from mysql_roku_firstplays_firstplay_date_date) as epoch_timestamp,
  case
    when '1111' :: int = 1111 then 1111
    else null
  end as platform_id
from looker.roku_firstplays
),

a as
(

select
  sent_at as timestamp,
  b.date as release_date,
  end_date,
  case
    when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection
  end as collection,
  case
    when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
    when series is not null then 'series'
    else 'other'
  end as type,
  cast(a.video_id as integer) as video_id,
  series,
  trim(b.title) as title,
  user_id,
  a.id as anonymous_id,
  'firstplay' as event_type,
  'Roku' as source,
  extract(epoch from sent_at) as epoch_timestamp,
  case
    when platform_id is not null then cast(platform_id as integer)
  end as platform_id,
  episode,
  null as tv_cast,
  a.duration,
  a.timecode
from roku.video_content_playing as a
left join titles_id_mapping as b
on cast(a.video_id as varchar) = cast(b.id as varchar)

)

select * from a

;;

distribution_style: "even"
sortkeys: ["user_id", "video_id"]
datagroup_trigger:redshift_upff_datagroup

  }
}
  # # You can specify the table name if it's different from the view name:
  # sql_table_name: my_schema_name.tester ;;
  #
  # # Define your dimensions and measures here, like this:
  # dimension: user_id {
  #   description: "Unique ID for each user that has ordered"
  #   type: number
  #   sql: ${TABLE}.user_id ;;
  # }
  #
  # dimension: lifetime_orders {
  #   description: "The total number of orders for each user"
  #   type: number
  #   sql: ${TABLE}.lifetime_orders ;;
  # }
  #
  # dimension_group: most_recent_purchase {
  #   description: "The date when each user last ordered"
  #   type: time
  #   timeframes: [date, week, month, year]
  #   sql: ${TABLE}.most_recent_purchase_at ;;
  # }
  #
  # measure: total_lifetime_orders {
  #   description: "Use this for counting lifetime orders across many users"
  #   type: sum
  #   sql: ${lifetime_orders} ;;
  # }

# view: redshift_allfirstplay_roku_video_content_playing {
#   # Or, you could make this view a derived table, like this:
#   derived_table: {
#     sql: SELECT
#         user_id as user_id
#         , COUNT(*) as lifetime_orders
#         , MAX(orders.created_at) as most_recent_purchase_at
#       FROM orders
#       GROUP BY user_id
#       ;;
#   }
#
#   # Define your dimensions and measures here, like this:
#   dimension: user_id {
#     description: "Unique ID for each user that has ordered"
#     type: number
#     sql: ${TABLE}.user_id ;;
#   }
#
#   dimension: lifetime_orders {
#     description: "The total number of orders for each user"
#     type: number
#     sql: ${TABLE}.lifetime_orders ;;
#   }
#
#   dimension_group: most_recent_purchase {
#     description: "The date when each user last ordered"
#     type: time
#     timeframes: [date, week, month, year]
#     sql: ${TABLE}.most_recent_purchase_at ;;
#   }
#
#   measure: total_lifetime_orders {
#     description: "Use this for counting lifetime orders across many users"
#     type: sum
#     sql: ${lifetime_orders} ;;
#   }
# }
