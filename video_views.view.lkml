view: video_views {
  derived_table: {
    sql:
    with a as
      (
      select
        video_title,
        collection_title,
        CAST(video_id as bigint) as video_id,
        user_id,
        anonymous_id,
        event as event_type,
        'GaitherTV Web' as source,
        'web' as device_type,
        'web' as device_os,
        CASE
              WHEN is_live = 0 THEN 'onDemand'
              ELSE 'Live'
          END AS presentation,
        is_live,
        context_timezone,
        session_id,
        EXTRACT(EPOCH FROM sent_at) as EPOCH_TIMESTAMP,
        cast(is_chromecast as bigint) as tv_cast,
        timestamp
      from javascript_gaither_tv_seller_site.video_content_playing

      union all

      select
        video_title,
        collection_title,
        cast(video_id as bigint) as video_id,
        user_id,
        anonymous_id,
        event as event_type,
        'GaitherTV Android' as source,
        device as device_type,
        CASE
              WHEN device = 'fire_tv' THEN 'fire tv'
              ELSE context_device_type
          END AS device_os,
        CASE
              WHEN is_live = 0 THEN 'onDemand'
              ELSE 'Live'
          END AS presentation,
        is_live,
        context_timezone,
        session_id,
        EXTRACT(EPOCH FROM sent_at) as EPOCH_TIMESTAMP,
        cast(is_chromecast as bigint) as tv_cast,
        timestamp
      from gaither_tv_android.video_content_playing

      union all

      select

        video_title,
        collection_title,
        cast(video_id as bigint) as video_id,
        user_id,
        anonymous_id,
        event as event_type,
        'GaitherTV Apple' as source,
        device as device_type,
        context_device_type as device_os,
        CASE
              WHEN is_live = 0 THEN 'onDemand'
              ELSE 'Live'
          END AS presentation,
        is_live,
        context_timezone,
        session_id,
        EXTRACT(EPOCH FROM sent_at) as EPOCH_TIMESTAMP,
        cast(is_chromecast as bigint) as tv_cast,
        timestamp
      from gaither_tv_apple.video_content_playing

      union all

      select

        video_title,
        collection_title,
        cast(video_id as bigint) as video_id,
        user_id,
        anonymous_id,
        event as event_type,
        'GaitherTV FireTV' as source,
        'fire tv'as device_type,
        'fire tv' as device_os,
        CASE
              WHEN is_live = 0 THEN 'onDemand'
              ELSE 'Live'
          END AS presentation,
        is_live,
        context_timezone,
        session_id,
        EXTRACT(EPOCH FROM sent_at) as EPOCH_TIMESTAMP,
        cast(is_chromecast as bigint) as tv_cast,
        timestamp
      from gaither_tv_fire_tv.video_content_playing

      union all

      select
        video_title,
        collection_title,
        cast(video_id as bigint) as video_id,
        user_id,
        anonymous_id,
        event as event_type,
        'GaitherTV Roku' as source,
        'roku'as device_type,
        'roku' as device_os,
        CASE
              WHEN is_live = 0 THEN 'onDemand'
              ELSE 'Live'
          END AS presentation,
        is_live,
        NULL as context_timezone,
        session_id,
        EXTRACT(EPOCH FROM sent_at) as EPOCH_TIMESTAMP,
        cast(is_chromecast as bigint) as tv_cast,
        timestamp
      from gaither_tv_roku.video_content_playing
      ),

      /*
         filter invalid user_id
         filter null video_title
      */
      play_data_global as
      (
      select * from a
      where user_id <> '0'
      and user_id ~ '^[0-9]*$'
      and user_id is not null
      and video_title is not null
      ),

      plays_most_granular as
      (
      select
        user_id,
        row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count,
        min(timestamp) over (partition by user_id, video_id, date(timestamp)) as start_time,
        max(timestamp) over (partition by user_id, video_id, date(timestamp)) as end_time,
        date(timestamp) as view_dt,
        collection_title,
        video_title,
        video_id,
        source,
        device_os,
        device_type,
        is_live,
        presentation,
        context_timezone,
        session_id,
        timestamp
      from play_data_global
      order by
        user_id, date(timestamp), video_id, min_count
      ),

      plays_max_duration as
      (
      select
        user_id,
        video_id,
        date(timestamp) as date,
        max(min_count) as min_count
      from plays_most_granular
      group by 1,2,3
      ),

      plays_less_granular as
      (
      select
        a.*,
        row_number() over (partition by a.user_id order by a.timestamp) as play_number
      from plays_most_granular as a
      inner join plays_max_duration as b
      on a.user_id = b.user_id
      and a.video_id = b.video_id
      and date(a.timestamp) = b.date
      and a.min_count = b.min_count
      ),

      result as (
      select
        user_id as customer_id
        ,device_os
        ,device_type
        ,is_live as is_live_channel
        ,CASE
  WHEN TRY_CAST(start_time AS TIMESTAMP) IS NOT NULL
       AND TRY_CAST(context_timezone AS VARCHAR) IS NOT NULL THEN
    (start_time AT TIME ZONE 'UTC') AT TIME ZONE context_timezone
  ELSE
    NULL
END AS local_view_time
        ,presentation
        ,session_id
        ,context_timezone as time_zone
        ,start_time
        ,end_time
        ,video_id
        ,min_count * 10 as duration
        --,timestamp_diff(end_time,start_time,SECOND) AS time_difference
      from plays_less_granular)


      select *
      FROM result ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: device_os {
    type: string
    sql: ${TABLE}.device_os ;;
  }

  dimension: device_type {
    type: string
    sql: ${TABLE}.device_type ;;
  }

  dimension: is_live_channel {
    type: number
    sql: ${TABLE}.is_live_channel ;;
  }

  dimension_group: local_view_time {
    type: time
    sql: ${TABLE}.local_view_time ;;
  }

  dimension: presentation {
    type: string
    sql: ${TABLE}.presentation ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: time_zone {
    type: string
    sql: ${TABLE}.time_zone ;;
  }

  dimension: start_time {
    type: string
    sql: ${TABLE}.start_time ;;
  }

  dimension: end_time {
    type: string
    sql: ${TABLE}.end_time ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  set: detail {
    fields: [
      customer_id,
      device_os,
      device_type,
      is_live_channel,
      local_view_time_time,
      presentation,
      session_id,
      time_zone,
      start_time,
      end_time,
      video_id,
      duration
    ]
  }
}
