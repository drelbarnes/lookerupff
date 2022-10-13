view: search_and_discovery2 {
    derived_table: {
      sql: with

      search_p0 as ( with

      search_ios as (
      select user_id, date(timestamp) as date_stamp,
      search_query, context_device_type as platform, case
      when lower(context_device_name) = 'ipod touch' then 'iphone'
      else lower(regexp_replace(context_device_name, ' ', '_')) end as device, timestamp, event,
      row_number() over (partition by user_id order by timestamp) as device_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      row_number() over (partition by user_id, search_query order by timestamp) as title_seq_num
      from ios.search_executed
      where search_query is not null
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      search_roku as (
      select user_id, date(timestamp) as date_stamp,
      search_query, 'roku' as platform,
      device as device, timestamp, event,
      row_number() over (partition by user_id order by timestamp) as device_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      row_number() over (partition by user_id, search_query order by timestamp) as title_seq_num
      from roku.search_executed
      where search_query is not null
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      search_android as (
      select user_id, date(timestamp) as date_stamp,
      search_query, context_device_type as platform,
      device as device, timestamp, event,
      row_number() over (partition by user_id order by timestamp) as device_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      row_number() over (partition by user_id, search_query order by timestamp) as title_seq_num
      from android.search_executed
      where search_query is not null
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      search_javascript as (
      select user_id, date(timestamp) as date_stamp,
      search_query, 'web' as platform,
      device as device, timestamp, event,
      row_number() over (partition by user_id order by timestamp) as device_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      row_number() over (partition by user_id, search_query order by timestamp) as title_seq_num
      from javascript.search_executed
      where search_query is not null
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      search_amazon as (
      select user_id, date(timestamp) as date_stamp,
      search_query, 'amazon' as platform,
      'fire_tv' as device, timestamp, event,
      row_number() over (partition by user_id order by timestamp) as device_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      row_number() over (partition by user_id, search_query order by timestamp) as title_seq_num
      from amazon_fire_tv.search_executed
      where search_query is not null
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      search_all_platforms_p0 as (
      select * from search_ios union all
      select * from search_roku union all
      select * from search_android union all
      select * from search_javascript union all
      select * from search_amazon),

      search_all_platforms_p1 as (
      select *, row_number() over (
      partition by user_id order by timestamp)
      as search_seq_num from search_all_platforms_p0),

      search_count_by_user as (
      select user_id,
      max(search_seq_num) as total_search_count,
      max(daily_seq_num) as max_daily_search_count,
      avg(daily_seq_num) as avg_daily_search_count
      from search_all_platforms_p1
      group by user_id)

      select a.*,
      b.total_search_count,
      b.max_daily_search_count,
      b.avg_daily_search_count
      from search_all_platforms_p1 as a
      left join search_count_by_user as b
      on a.user_id = b.user_id
      ),

      result_p0 as ( with

      result_ios as (
      select user_id, context_traits_email as email,
      video_id, video_title, collection_title,
      'ios' as platform, case
      when platform = 'tvos' then 'apple_tv' else platform end as device,
      event, view, date(timestamp) as date_stamp, timestamp,
      row_number() over (partition by user_id order by timestamp) as platform_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      from ios.search_result_selected
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      result_roku as (
      select user_id, context_traits_email as email,
      video_id, video_title, collection_title,
      'roku' as platform, platform as device,
      event, view, date(timestamp) as date_stamp, timestamp,
      row_number() over (partition by user_id order by timestamp) as platform_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      from roku.search_result_selected
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      result_android as (
      select user_id, context_traits_email as email,
      video_id, video_title, collection_title,
      'android' as platform, platform as device,
      event, view, date(timestamp) as date_stamp, timestamp,
      row_number() over (partition by user_id order by timestamp) as platform_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      from android.search_result_selected
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      result_javascript as (
      select user_id, context_traits_email as email,
      video_id, video_title, collection_title,
      'web' as platform, platform as device,
      event, view, date(timestamp) as date_stamp, timestamp,
      row_number() over (partition by user_id order by timestamp) as platform_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      from javascript.search_result_selected
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      result_amazon as (
      select user_id, context_traits_email as email,
      video_id, video_title, collection_title,
      'amazon' as platform, platform as device,
      event, view, date(timestamp) as date_stamp, timestamp,
      row_number() over (partition by user_id order by timestamp) as platform_seq_num,
      row_number() over (partition by user_id, date(timestamp) order by timestamp) as daily_seq_num,
      from amazon_fire_tv.search_result_selected
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and user_id <> '0'),

      result_all_platforms_p0 as (
      select * from result_ios union all
      select * from result_roku union all
      select * from result_android union all
      select * from result_javascript union all
      select * from result_amazon),

      result_all_platforms_p1 as (
      select *, row_number() over (
      partition by user_id order by timestamp)
      as result_seq_num from result_all_platforms_p0),

      search_count_by_user as (
      select user_id,
      max(result_seq_num) as total_result_count,
      max(daily_seq_num) as max_daily_result_count,
      avg(daily_seq_num) as avg_daily_result_count
      from result_all_platforms_p1
      group by user_id),

      result_all_platforms_p2 as (
      select a.*,
      b.total_result_count,
      b.max_daily_result_count,
      b.avg_daily_result_count
      from result_all_platforms_p1 as a
      left join search_count_by_user as b
      on a.user_id = b.user_id),

      titles as ( with

      titles_p0 as (
      select distinct video_id, title,
      metadata_series_name as series,
      metadata_season_number as season,
      row_number() over (partition by video_id) as row,
      from php.get_titles)

      select * from titles_p0 where row = 1)

      select a.user_id, a.video_id,
      coalesce(a.video_title, b.title) as video_time,
      coalesce(b.series, 'Movie') as collection_title,
      coalesce(b.season, 0) as season,
      platform, device, view, date_stamp, timestamp,
      total_result_count,
      max_daily_result_count,
      avg_daily_result_count,
      platform_seq_num,
      result_seq_num, event
      from result_all_platforms_p2 as a left join titles as b
      on a.video_id = b.video_id
      where a.video_id is not null
      ),

      search as (select * except (event) from search_p0),
      result as (select * except (event) from result_p0)

      select * from result
      ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

  measure: user_count {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
  }

  measure: result_count {
    type: max
    sql: ${TABLE}.result_seq_num ;;
  }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: video_time {
      type: string
      sql: ${TABLE}.video_time ;;
    }

    dimension: collection_title {
      type: string
      sql: ${TABLE}.collection_title ;;
    }

    dimension: season {
      type: number
      sql: ${TABLE}.season ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: device {
      type: string
      sql: ${TABLE}.device ;;
    }

    dimension: view {
      type: string
      sql: ${TABLE}.view ;;
    }

    dimension: date_stamp {
      type: date
      datatype: date
      sql: ${TABLE}.date_stamp ;;
    }

    dimension_group: timestamp {
      type: time
      sql: ${TABLE}.timestamp ;;
    }

    dimension: total_result_count {
      type: number
      sql: ${TABLE}.total_result_count ;;
    }

    dimension: max_daily_result_count {
      type: number
      sql: ${TABLE}.max_daily_result_count ;;
    }

    dimension: avg_daily_result_count {
      type: number
      sql: ${TABLE}.avg_daily_result_count ;;
    }

    dimension: platform_seq_num {
      type: number
      sql: ${TABLE}.platform_seq_num ;;
    }

    dimension: result_seq_num {
      type: number
      sql: ${TABLE}.result_seq_num ;;
    }

    set: detail {
      fields: [
        user_id,
        video_id,
        video_time,
        collection_title,
        season,
        platform,
        device,
        view,
        date_stamp,
        timestamp_time,
        total_result_count,
        max_daily_result_count,
        avg_daily_result_count,
        platform_seq_num,
        result_seq_num
      ]
    }
  }
