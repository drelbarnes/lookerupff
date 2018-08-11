view: top_play {
  derived_table: {
    sql: with a as
        (select a.timestamp,
                case when series is not null then trim(upper(split_part(series,'|',1))) else trim(upper(title)) end as series,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,
                trim(upper(title)) as title,
                user_id,
                'Android' as platform
         from android.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,
                case when series is not null then trim(upper(split_part(series,'|',1))) else trim(upper(title)) end as series,
                cast(a.video_id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,trim(upper(title)) as title,
                user_id,
                'IOS' as platform
         from ios.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id
         union all
         select a.timestamp,
                case when series is not null then trim(upper(split_part(series,'|',1))) else trim(upper(split_part(a.title,'-',1))) end as series,
                cast(b.id as int) as video_id,
                trim(upper(split_part(season,'Season',2))) as season,
                episode,
                trim(upper(split_part(a.title,'-',1))) as title,
                user_id,
                'Web' as platform
         from javascript.firstplay as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title))=trim(upper(split_part(a.title,'-',1))) ),

b as
(select a.*, status
from a inner join customers.customers on user_id=customer_id  ),

c as
(SELECT
  video_id,
  COUNT(*) AS "all_play.count"
FROM b
WHERE ((title IS NOT NULL)) AND (b.timestamp  >= TIMESTAMP '2018-04-10')
GROUP BY 1
ORDER BY 2 DESC
limit 500)

select *,
       case when series=title then title else series||' - '||title end as series_title from b where video_id in (select video_id from c) ;;
  }
  dimension: title {
    type: string
    sql: ${TABLE}.title;;
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

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension: series_title {
    type: string
    sql: ${TABLE}.series_title ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
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
