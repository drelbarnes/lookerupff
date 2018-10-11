view: all_firstplay {
  derived_table: {
    sql:

    (with a as
        (select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(title)) as title,
                user_id,
                c.platform,
                'Android' as source
         from android.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
         on a.user_id = c.customer_id
         union all
         select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(title)) as title,
                user_id,
                c.platform,
                'iOS' as source
         from ios.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
         on a.user_id = c.customer_id
         union all
         select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(b.id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(split_part(a.title,'-',1))) as title,
                user_id,
                c.platform,
                'Web' as source
         from javascript.firstplay as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title)) = trim(upper(split_part(a.title,'-',1)))
         left join customers.customers as c
         on a.user_id = c.customer_id
        )

select a.*, status
from a inner join customers.customers on user_id = customer_id);;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: string
    sql: ${TABLE}.episode ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: release_date {
    type: date
    sql: ${TABLE}.release_date ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${source};;
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      title,
      platform,
      user_id
    ]
  }
}
