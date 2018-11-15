view: bigquery_topseries {
  derived_table: {
    sql: WITH bigquery_allfirstplay AS (with a1 as
      (select sent_at as timestamp,
              user_id,
              (split(title," - ")) as title
      from javascript.firstplay),

      a2 as
      (select timestamp,
              user_id,
              title[safe_ordinal(1)] as title
       from a1 order by 1),

      a as
              (select sent_at as timestamp,
                      b.date as release_date,
                      collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(a.video_id as int64) as video_id,
                      trim((title)) as title,
                      user_id,
                      c.platform,
                      'Android' as source
               from android.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
               on safe_cast(a.user_id as int64) = c.customer_id
               union all
               select sent_at as timestamp,
                      b.date as release_date,
                      collection,
                      case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(a.video_id as int64) as video_id,
                      trim((title)) as title,
                      user_id,
                      c.platform,
                      'iOS' as source
               from ios.firstplay as a left join svod_titles.titles_id_mapping as b on safe_cast(a.video_id as int64) = b.id left join customers.customers as c
               on safe_cast(a.user_id as int64) = c.customer_id
               union all
               select timestamp,
                      b.date as release_date,
                      collection,
                      case when series is null and upper(collection)=upper(b.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
                      cast(b.id as int64) as video_id,
                      trim(b.title) as title,
                      user_id,
                      c.platform,
                      'Web' as source
               from a2 as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title)) = a.title
               left join customers.customers as c
               on safe_cast(a.user_id as int64) = c.customer_id)


      select a.*
      from a),

      t as
      (SELECT
        bigquery_allfirstplay.title AS bigquery_allfirstplay_title,
        COUNT(*) AS bigquery_allfirstplay_count
      FROM bigquery_allfirstplay

      WHERE (bigquery_allfirstplay.type = 'series') AND (((bigquery_allfirstplay.timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -29 DAY))) AND (bigquery_allfirstplay.timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -29 DAY), INTERVAL 30 DAY)))))
      GROUP BY 1
      ORDER BY 2 DESC
      LIMIT 50)

      select a.* from bigquery_allfirstplay as a inner join t on a.title=bigquery_allfirstplay_title
       ;;
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
