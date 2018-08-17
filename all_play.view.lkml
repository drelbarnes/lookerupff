view: all_play {
  derived_table: {
    sql:
    with a1 as

(with
a as
(select a.timestamp,
                case when series is not null then trim(upper(split_part(series,'|',1))) else trim(upper(split_part(a.title,'-',1))) end as series,
                case when series is not null then trim(upper(split_part(a.title,'-',3))) else null end as season,
                episode,
                trim(upper(split_part(a.title,'-',1))) as title,
                cast(b.id as int) as video_id
         from javascript.firstplay as a inner join svod_titles.titles_id_mapping as b on trim(upper(b.title))=trim(upper(split_part(a.title,'-',1))) ),

b as
(select distinct series,
                    season,
                    title,
                    episode,
                    video_id
from a
where season <> ''
order by series, season, episode),

c as
(select distinct series,
       'Movie' as season,
       title,
       episode,
       video_id
from a
where season is null)

select series||' - '||season as collection, * from b
union all
select season||' - '||series as collection, * from c),

 d as
(select a.timestamp,
                collection,
                series,
                season,
                title,
                episode,
                b.video_id,
                user_id,
                'Android' as platform
         from android.firstplay as a inner join a1 as b on a.video_id=b.video_id
         union all
         select a.timestamp,
                collection,
                series,
                season,
                title,
                episode,
                b.video_id,
                user_id,
                'IOS' as platform
         from ios.firstplay as a inner join a1 as b on a.video_id=b.video_id
         union all
         select a.timestamp,
                collection,
                series,
                season,
                b.title,
                episode,
                b.video_id,
                user_id,
                'Web' as platform
         from javascript.firstplay as a inner join a1 as b on trim(upper(b.title))=trim(upper(split_part(a.title,'-',1))) )

select d.*, status
from d left join customers.customers on user_id=customer_id
;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
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
