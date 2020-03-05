view: bigquery_personas_v2 {
  derived_table: {
    sql: with a as
(select user_id,
       max(status_date) as status_date
from http_api.purchase_event
group by 1),

b as
(select user_id,
       topic,
       email,
       max(status_date) as status_date
from http_api.purchase_event
group by 1,2,3),

c as
(select distinct b.user_id,
                 b.email
from a inner join b on a.user_id=b.user_id and a.status_date=b.status_date
where topic not in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired','customer.product.charge_failed','customer.created')),

d as
(select c.*,
       case when string_field_1='F' then 1 else 0 end as women,
       case when string_field_1='M' then 1 else 0 end as men,
       case when sum(case when platform='web' then 1 else 0 end)>0 then 1 else 0 end as web,
       case when sum(case when platform='android' then 1 else 0 end)>0 then 1 else 0 end as android,
       case when sum(case when platform='ios' then 1 else 0 end)>0 then 1 else 0 end as ios,
       case when sum(case when platform='roku' then 1 else 0 end)>0 then 1 else 0 end as roku,
       sum(safe_cast(moptin as int64)) as marketing_optin,
       sum(case when topic in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired') then 1 else 0 end) as churns,
       sum(case when topic in ('customer.product.renewed') then 1 else 0 end) as renewals
from c left join http_api.purchase_event as d on c.user_id=d.user_id left join svod_titles.gender on fname=string_field_0
group by 1,2,3,4),

titles_id_mapping as
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
from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id
 where date(ingest_at)>='2020-02-13' and (metadata_series_name is not null or metadata_movie_name is not null)),

-- awl as
-- (SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_older
-- FROM javascript.addwatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_older
-- FROM android.addwatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_older
-- FROM ios.addwatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as addwatchlist_older
-- FROM roku.addwatchlist where user_id is not null),

-- rwl as
-- (SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_older
-- FROM javascript.removewatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_older
-- FROM android.removewatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_older
-- FROM ios.removewatchlist where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as removewatchlist_older
-- FROM roku.removewatchlist where user_id is not null),


-- error as
-- (SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as error_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as error_older
-- FROM javascript.error where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as error_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as error_older
-- FROM android.error where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as error_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as error_older
-- FROM ios.error where user_id is not null
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as error_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as error_older
-- FROM roku.error where user_id is not null ),

-- view as
-- (SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as view_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as view_older
-- FROM javascript.view where user_id is not null and date(timestamp)>date_sub(current_date(),interval 3 month)
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as view_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as view_older
-- FROM android.view where user_id is not null and date(timestamp)>date_sub(current_date(),interval 3 month)
-- union all
-- SELECT safe_cast(user_id as string) as user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as view_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as view_older
-- FROM ios.view where user_id is not null and date(timestamp)>date_sub(current_date(),interval 3 month)
-- union all
-- SELECT user_id,
--        case when date(timestamp)>date_sub(current_date(),interval 30 day) then 1 else 0 end as view_recent,
--        case when date(timestamp)<=date_sub(current_date(),interval 30 day) then 1 else 0 end as view_older
-- FROM roku.view where user_id is not null and date(timestamp)>date_sub(current_date(),interval 3 month)),

web AS (
            SELECT
              b.user_id,
              collection,
              b.video_id,
              date(b.timestamp) as timestamp,
              max(timecode/3600) as duration
            FROM
              javascript.video_content_playing as b inner join titles_id_mapping as a on b.video_id=a.id
            group by 1,2,3,4),

            droid AS (
            SELECT
              b.user_id,
              collection,
              b.video_id,
              date(b.timestamp) as timestamp,
              max(timecode/3600) as duration
            FROM android.video_content_playing as b inner join titles_id_mapping as a on b.video_id=a.id
            group by 1,2,3,4),

            roku AS (
            SELECT
              b.user_id,
              collection,
              b.video_id,
              date(b.timestamp) as timestamp,
              max(timecode/3600) as duration
            FROM roku.video_content_playing as b inner join titles_id_mapping as a on b.video_id=a.id
            group by 1,2,3,4),

            apple AS (
            SELECT
              b.user_id,
              collection,
              b.video_id,
              date(b.timestamp) as timestamp,
              max(timecode/3600) as duration
            FROM ios.video_content_playing as b inner join titles_id_mapping as a on b.video_id=safe_cast(a.id as string)
            group by 1,2,3,4),

            all1 as
            (select user_id,
                   collection,
                   timestamp,
                   duration
            from web
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration
            from droid
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration
            from apple
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration
            from roku),

            fp as
            (select user_id,
              sum(case WHEN collection LIKE '%Heartland%' and (timestamp)>date_sub(current_date(),interval 30 day) then duration else 0 end) as heartland_recent,
              sum(case WHEN collection LIKE '%Heartland%' and (timestamp)<=date_sub(current_date(),interval 30 day) then duration else 0 end) as heartland_older,
              sum(case WHEN collection LIKE '%Bates%' and (timestamp)>date_sub(current_date(),interval 30 day) then duration else 0 end) as bates_recent,
              sum(case WHEN collection LIKE '%Bates%' and (timestamp)<=date_sub(current_date(),interval 30 day) then duration else 0 end) as bates_older,
              sum(case WHEN collection not LIKE '%Heartland%' and collection not LIKE '%Bates%' and (timestamp)>date_sub(current_date(),interval 30 day) then duration else 0 end) as other_recent,
              sum(case WHEN collection not LIKE '%Heartland%' and collection not LIKE '%Bates%' and (timestamp)<=date_sub(current_date(),interval 30 day) then duration else 0 end) as other_older
            from all1
            group by 1)

-- awl_sum as
-- (select user_id,
--        sum(addwatchlist_recent) as addwatchlist_recent,
--        sum(addwatchlist_older) as addwatchlist_older
-- from awl
-- group by 1),

-- rwl_sum as
-- (select user_id,
--        sum(removewatchlist_recent) as removewatchlist_recent,
--        sum(removewatchlist_older) as removewatchlist_older
-- from rwl
-- group by 1),

-- error_sum as
-- (select user_id,
--         sum(error_recent) as error_recent,
--         sum(error_older) as error_older
-- from error
-- group by 1)

-- view_sum as
-- (select user_id,
--         sum(view_recent) as view_recent,
--         sum(view_older) as view_older
-- from view
-- group by 1)

(select distinct
       d.*,
--        case when addwatchlist_recent is null then 0 else addwatchlist_recent end as addwatchlist_recent,
--        case when addwatchlist_older is null then 0 else addwatchlist_older end as addwatchlist_older,
--        case when removewatchlist_recent is null then 0 else removewatchlist_recent end as removewatchlist_recent,
--        case when removewatchlist_older is null then 0 else removewatchlist_older end as removewatchlist_older,
--       case when error_recent is null then 0 else error_recent end as error_recent,
--       case when error_older is null then 0 else error_older end as error_older,
--        view_recent,
--        view_older,
       case when heartland_recent is null then 0 else heartland_recent end as heartland_recent,
       case when heartland_older is null then 0 else heartland_older end as heartland_older,
       case when bates_recent is null then 0 else bates_recent end as bates_recent,
       case when bates_older is null then 0 else bates_older end as bates_older,
       case when other_recent is null then 0 else other_recent end as other_recent,
       case when  other_older is null then 0 else other_older end as other_older
from d
-- left join awl on d.user_id=awl.user_id
--       left join error on d.user_id=error.user_id
--        left join view on d.user_id=view.user_id
--        left join rwl on d.user_id=rwl.user_id
       left join fp on d.user_id=fp.user_id) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: women {
    type: number
    sql: ${TABLE}.women ;;
  }

  dimension: men {
    type: number
    sql: ${TABLE}.men ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: marketing_optin {
    type: string
    sql: ${TABLE}.marketing_optin ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: web {
    type: number
    sql: ${TABLE}.web ;;
  }

  dimension: android {
    type: number
    sql: ${TABLE}.android ;;
  }

  dimension: ios {
    type: number
    sql: ${TABLE}.ios ;;
  }

  dimension: roku {
    type: number
    sql: ${TABLE}.roku ;;
  }

  dimension: churns {
    type: number
    sql: ${TABLE}.churns ;;
  }

  dimension: renewals {
    type: number
    sql: ${TABLE}.renewals ;;
  }

  dimension: addwatchlist_recent {
    type: number
    sql: ${TABLE}.addwatchlist_recent ;;
  }

  dimension: addwatchlist_older {
    type: number
    sql: ${TABLE}.addwatchlist_older ;;
  }

  dimension: removewatchlist_recent {
    type: number
    sql: ${TABLE}.removewatchlist_recent ;;
  }

  dimension: removewatchlist_older {
    type: number
    sql: ${TABLE}.removewatchlist_older ;;
  }

#   dimension: error_recent {
#     type: number
#     sql: ${TABLE}.error_recent ;;
#   }
#
#   dimension: error_older {
#     type: number
#     sql: ${TABLE}.error_older ;;
#   }

  dimension: heartland_recent {
    type: number
    sql: ${TABLE}.heartland_recent ;;
  }

  dimension: heartland_older {
    type: number
    sql: ${TABLE}.heartland_older ;;
  }

  dimension: bates_recent {
    type: number
    sql: ${TABLE}.bates_recent ;;
  }

  dimension: bates_older {
    type: number
    sql: ${TABLE}.bates_older ;;
  }

  dimension: other_recent {
    type: number
    sql: ${TABLE}.other_recent ;;
  }

  dimension: other_older {
    type: number
    sql: ${TABLE}.other_older ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      web,
      android,
      ios,
      roku,
      churns,
      renewals,
      addwatchlist_recent,
      addwatchlist_older,
      removewatchlist_recent,
      removewatchlist_older,
#       error_recent,
#       error_older,
      heartland_recent,
      heartland_older,
      bates_recent,
      bates_older,
      other_recent,
      other_older
    ]
  }
}
