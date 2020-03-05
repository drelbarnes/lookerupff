view: customer_segmentation {
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
       date(created_at) as created_at,
       max(status_date) as status_date
from http_api.purchase_event
group by 1,2,3,4),

c as
(select distinct b.user_id,
                 b.email,
                 created_at,
                 b.status_date
from a inner join b on a.user_id=b.user_id and a.status_date=b.status_date
where topic not in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired','customer.product.charge_failed','customer.created')),

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
                   duration,
                   'web' as platform
            from web
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration,
                   'android' as platform
            from droid
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration,
                   'ios' as platform
            from apple
            union all
            select user_id,
                   collection,
                   timestamp,
                   duration,
                   'roku' as platform
            from roku),

            fp as
            (select user_id,
              sum(case when platform='web' then 1 else 0 end) as web_views,
              sum(case when platform='android' then 1 else 0 end) as android_views,
              sum(case when platform='ios' then 1 else 0 end) as ios_views,
              sum(case when platform='roku' then 1 else 0 end) as roku_views,
              sum(case WHEN (timestamp)>date_sub(current_date(),interval 15 day) then 1 else 0 end) as recent_views,
              sum(case WHEN collection LIKE '%Heartland%'  then 1 else 0 end) as heartland_views,
              sum(case WHEN (timestamp) between date_sub(current_date(),interval 28 day) and date_sub(current_date(),interval 15 day) then 1 else 0 end) as semi_recent_views,
              sum(case WHEN collection LIKE '%Bates%'  then 1 else 0 end) as bates_views,
              sum(case WHEN (timestamp)<date_sub(current_date(),interval 28 day) then 1 else 0 end) as laggard_views,
              sum(case WHEN collection not LIKE '%Heartland%' and collection not LIKE '%Bates%' then 1 else 0 end) as other_content_views
            from all1
            where user_id is not null
            group by 1),

current_customers as
(select email,
        'current customer' as customer_type,
        created_at,
        date(status_date) as status_date,
        fp.*
from c left join fp on c.user_id=fp.user_id),

c1 as
(select distinct b.user_id,
                 b.email,
                 created_at,
                 b.status_date
from a inner join b on a.user_id=b.user_id and a.status_date=b.status_date
where topic in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired','customer.product.charge_failed','customer.created')),

prior_customers as
(select email,
        'previous customer' as customer_type,
        created_at,
        date(status_date) as status_date,
        fp.*
from c1 left join fp on c1.user_id=fp.user_id),

e as
(select distinct user_id,
       date(status_date) as status_date,
       date(created_at) as created_at,
       date_diff(date(status_date),date(created_at), day) as tenure
from http_api.purchase_event
where topic in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired')),

e1 as
(select user_id,
        sum(tenure) as tenure,
        max(status_date) as status_date
from e
group by 1)

(select a.*,
       case when e1.user_id is null then 0 else 1 end as resubscriber,
       case when e1.user_id is null then date_diff(current_date(),created_at,day) else tenure+date_diff(current_date(),a.status_date,day) end as tenure_days
from current_customers as a left join e1 on a.user_id=e1.user_id
union all
select a.*,
       0 as resubscriber,
       tenure as tenure_days
from prior_customers as a left join e1 on a.user_id=e1.user_id)

       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: customer_type {
    type: string
    sql: ${TABLE}.customer_type ;;
  }

  dimension: created_at {
    type: date
    sql: ${TABLE}.created_at ;;
  }

  dimension: status_date {
    type: date
    sql: ${TABLE}.status_date ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: web_views {
    type: number
    sql: ${TABLE}.web_views ;;
  }

  dimension: android_views {
    type: number
    sql: ${TABLE}.android_views ;;
  }

  dimension: ios_views {
    type: number
    sql: ${TABLE}.ios_views ;;
  }

  dimension: roku_views {
    type: number
    sql: ${TABLE}.roku_views ;;
  }

  dimension: recent_views {
    type: number
    sql: ${TABLE}.recent_views ;;
  }

  dimension: heartland_views {
    type: number
    sql: ${TABLE}.heartland_views ;;
  }

  dimension: semi_recent_views {
    type: number
    sql: ${TABLE}.semi_recent_views ;;
  }

  dimension: bates_views {
    type: number
    sql: ${TABLE}.bates_views ;;
  }

  dimension: laggard_views {
    type: number
    sql: ${TABLE}.laggard_views ;;
  }

  dimension: other_content_views {
    type: number
    sql: ${TABLE}.other_content_views ;;
  }

  dimension: resubscriber {
    type: number
    sql: ${TABLE}.resubscriber ;;
  }

  dimension: tenure_days {
    type: number
    sql: ${TABLE}.tenure_days ;;
  }

  set: detail {
    fields: [
      email,
      customer_type,
      created_at,
      status_date,
      user_id,
      web_views,
      android_views,
      ios_views,
      roku_views,
      recent_views,
      heartland_views,
      semi_recent_views,
      bates_views,
      laggard_views,
      other_content_views,
      resubscriber,
      tenure_days
    ]
  }
}
