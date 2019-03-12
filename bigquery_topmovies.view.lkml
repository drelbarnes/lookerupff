view: bigquery_topmovies {
  derived_table: {
    sql:  WITH bigquery_allfirstplay AS (with a1 as
      (select sent_at as timestamp,
              user_id,
              (split(title," - ")) as title
      from javascript.firstplay),

      a2 as
      (select timestamp,
              user_id,
              title[safe_ordinal(1)] as title
       from a1 order by 1),

      a32 as
(select max(sent_at) as maxsentat from looker.roku_firstplays),

      a as
              (select sent_at as timestamp,
                      b.date as release_date,
                      1 as status_1,
                      collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(a.video_id as int64) as video_id,
                      trim((title)) as title1,
                      user_id,
                      'Android' as source
               from android.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
               on safe_cast(a.user_id as int64) = c.customer_id

               union all

               select mysql_roku_firstplays_firstplay_date_date  as timestamp,
                      b.date as release_date,
                      1 as status_1,
                      collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      mysql_roku_firstplays_video_id as video_id,
                      trim((title)) as title1,
                      user_id,
                      'Roku' as source
               from looker.roku_firstplays as a left join svod_titles.titles_id_mapping as b on mysql_roku_firstplays_video_id = b.id,a32
               where date(sent_at)=date(maxsentat)

               union all
               select sent_at as timestamp,
                      b.date as release_date,
                      1 as status_1,
                      collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(a.video_id as int64) as video_id,
                      trim((title)) as title1,
                      user_id,
                      'iOS' as source
               from ios.firstplay as a left join svod_titles.titles_id_mapping as b on safe_cast(a.video_id as int64) = b.id
               union all
               select timestamp,
                      b.date as release_date,
                      1 as status_1,
                      collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(b.id as int64) as video_id,
                      trim(b.title) as title1,
                      user_id,
                      'Web' as source
               from a2 as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))


      select a.*
      from a),

      t as
      (SELECT
        bigquery_allfirstplay.title1 AS bigquery_allfirstplay_title,
        1 as status_2,
        COUNT(DISTINCT concat(safe_cast(video_id as string),user_id,cast((CAST(timestamp  AS DATE)) as string)) ) AS bigquery_allfirstplay_count
      FROM bigquery_allfirstplay

      WHERE (bigquery_allfirstplay.type = 'movie') AND (((bigquery_allfirstplay.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -13 DAY))) AND (bigquery_allfirstplay.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -13 DAY), INTERVAL 14 DAY)))))
      GROUP BY 1
      ORDER BY 3 DESC
      LIMIT 14)

      select a.*, case when status_1+status_2=2 then a.title1 else "All Other Movies" end as title  from bigquery_allfirstplay as a left join t on a.title1=bigquery_allfirstplay_title   ;;
  }

  dimension: current_date {
    type: date
    sql: current_date() ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: release_date {
    type: date
    sql: ${TABLE}.release_date ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: play_count {
    type: count_distinct
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string)) ;;
  }

  set: detail {
    fields: [
      timestamp_time,
      release_date,
      collection,
      type,
      video_id,
      title,
      user_id,
      platform,
      source
    ]
  }
}
