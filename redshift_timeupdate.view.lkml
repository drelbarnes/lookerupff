view: redshift_timeupdate {
    derived_table: {
      sql: {% raw %} with

              a1 as
              ( -- Gathers most recent metadata ingestion
              select
                video_id
                , max(ingest_at) as loaded_at
              from php.get_titles
              group by 1
              ),

              a2 as
              ( -- Metadata collection and title classification
              select distinct
                metadata_series_name as series
                , case
                  when metadata_season_name in ('Season 1','Season 2','Season 3') then metadata_series_name || '-' || metadata_season_name
                  when metadata_season_name is null then metadata_movie_name
                  else metadata_season_name
                end as collection
                , media_type
                , metadata_season_number as season
                , a.title
                , a.video_id as id
                , episode_number as episode
                , date(time_available) as date
                , date(time_unavailable) as end_date
                , round(duration_seconds/60) as duration
                , '' as promotion
              from php.get_titles as a
              left join svod_titles.titles_id_mapping as b
              on a.video_id = b.id
              inner join a1
              on a1.video_id = a.video_id
              and a1.loaded_at = a.ingest_at
              ),

              a3 as
              ( -- Collects data from Xbox device
              select
                report_date as timestamp
                , video_id
                , user_id
                , sum(cast(total_minutes_watched as decimal(18,2))) as total_minutes_watched
              from php.get_xbox_views
              group by 1,2,3
              ),

              a4 as (

              ( -- Source 1: javascript.durationchange
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                , 'Web' as source
              from javascript.durationchange as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12
              )

              union all

              ( -- Source 2: ios.timeupdate
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie'  when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                , 'iOS' AS source
              from ios.timeupdate as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12
              )

              union all

              ( -- Source 3: ios.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                , 'iOS' AS source
              from ios.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12
              )

              union all

              ( -- Source 4: roku.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Roku' AS source
              from roku.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 5: android.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Android' AS source
              from android.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 6: amazon_fire_tv.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'FireTV' AS source
              from amazon_fire_tv.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 7: javascript.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Web' AS source
              from javascript.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 8: roku.timeupdate
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Roku' AS source
              from roku.timeupdate as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 9: android.timeupdate
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Android' AS source
              from android.timeupdate as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 10: lg_tv.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'LG' AS source
              from lg_tv.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 11: vizio_tv.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Vizio' AS source
              from vizio_tv.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 12: tizen_tv.video_content_playing
              select
                b.title
                , b.date as release
                , a.user_id
                , b.media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie' when series is not null then 'series' else 'other' end as type
                , cast(a.sent_at as date) :: timestamp AS timestamp
                , b.duration * 60 as duration
                , max(timecode) as timecode
                ,'Tizen' AS source
              from tizen_tv.video_content_playing as a
              left join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              group by 1,2,3,4,5,6,7,8,9,10,11,12)

              union all

              ( -- Source 13: php.get_xbox_views
              select  distinct
                b.title
                , b.date as release
                , a.user_id
                , media_type
                , cast(a.video_id as integer) as video_id
                , case when collection in ('Season 1','Season 2','Season 3') then series || ' ' || collection else collection end as collection
                , series
                , season
                , episode
                , case when series is null and upper(collection) = upper(b.title) then 'movie'  when series is not null then 'series' else 'other' end as type
                , a.timestamp
                , b.duration * 60 as duration
                , a.total_minutes_watched * 60 as timecode
                , 'Xbox' AS source
              from a3 as a
              inner join a2 as b
              on cast(a.video_id as integer) = b.id
              where a.user_id is not null
              and b.duration > 0
              )),

              timeupdate as
              ( -- Consolidates all play events across D2C device streams
              select distinct
                a.*
                , p.email
              from a4 as a
              inner join http_api.purchase_event as p
              on a.user_id = p.user_id
              )

              select * from timeupdate {% endraw %} ;;

      distribution_style: "even"
      sortkeys: ["user_id", "video_id"]
      datagroup_trigger: redshift_upff_datagroup
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: release {
      type: date
      sql: ${TABLE}.release ;;
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: media_type {
      type: string
      sql: ${TABLE}.media_type ;;
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    dimension: season {
      type: number
      sql: ${TABLE}.season ;;
    }

    dimension: episode {
      type: number
      sql: ${TABLE}.episode ;;
    }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    dimension_group: timestamp {
      type: time
      sql: ${TABLE}.timestamp ;;
    }

    dimension: duration {
      type: number
      sql: ${TABLE}.duration ;;
    }

    dimension: timecode {
      type: number
      sql: ${TABLE}.timecode ;;
    }

    dimension: source {
      type: string
      sql: ${TABLE}.source ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    measure: timecode_count {
      type: sum
      value_format: "0"
      sql: ${timecode} ;;
    }

    measure: duration_count {
      type: sum
      sql: ${duration} ;;
    }

    measure: minutes_watched {
      type: sum
      sql:
      case
        when ${duration} < ${timecode}
        then round(${duration} / 60)
        else round(${timecode} / 60)
      end ;;
      value_format: "#,##0"
    }

    measure: hours_watched {
      type: sum
      sql:
      case
        when ${duration} < ${timecode}
        then round(${duration} / 3600)
        else round(${timecode} / 3600)
      end ;;
      value_format: "#,##0"
    }

    dimension: hours_watched_dim {
      type: number
      sql:
      case
        when ${duration} < ${timecode}
        then round(${duration} / 3600)
        else round(${timecode} / 3600)
      end ;;
    value_format: "#,##0"
    }

    measure: hours_count {
      type: sum
      value_format: "#,##0"
      sql: ${hours_watched_dim} ;;
    }

    dimension: minutes_watched_dim {
      type: number
      sql:
      case
        when ${duration} < ${timecode}
        then round(${duration} / 60)
        else round(${timecode} / 60)
      end ;;
      value_format: "#,##0"
  }

    measure: minutes_count {
      type: sum
      value_format: "#,##0"
      sql: ${minutes_watched_dim} ;;
    }

    measure: percent_completed {
      type: number
      value_format: "0\%"
      sql: case when ${timecode_count} > ${duration_count} then 100.00 else 100.00 * ${timecode_count}/${duration_count} end ;;
    }

    measure: play_count {
      type: count_distinct
      sql: ${video_id}::varchar || ${user_id}::varchar || ${timestamp_date}::varchar ;;
      label: "Views"
    }

    measure: user_count {
      type: count_distinct
      sql: ${user_id} ;;
      label: "Uniques"
    }

    set: detail {
      fields: [
        title,
        release,
        user_id,
        media_type,
        video_id,
        collection,
        series,
        season,
        episode,
        type,
        timestamp_time,
        duration,
        timecode,
        source,
        email
      ]
    }
}
