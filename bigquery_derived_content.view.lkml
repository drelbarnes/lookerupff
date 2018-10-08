view: bigquery_derived_content {
  derived_table: {
    sql: with javascriptfirstplay as

(select a.timestamp,
        user_id,
        title as name,
        nth_value(split(title,'-'),1) over w1 as title
from javascript.firstplay as a
window w1 AS (
    PARTITION BY title ORDER BY title ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),

a as
(select         a.timestamp,
                user_id,
                case when series is not null then series else name end as series,
                season,
                episode,
                name,
                safe_cast(b.id as int64) as video_id
         from javascriptfirstplay as a inner join svod_titles.titles_id_mapping as b on trim(upper(b.title))=trim(upper(a.title[safe_ordinal(1)])) ),

b as
(select distinct series,
                    season,
                    name as title,
                    episode,
                    video_id
from a
where season <> ''
order by series, season, episode),

c as
(select distinct series,
       'Movie' as season,
       name as title,
       episode,
       video_id
from a
where season is null),

a1 as
(select  * from b
union all
select* from c),

d as
(select a.timestamp,
                series,
                season,
                title,
                episode,
                b.video_id,
                user_id,
                'Android' as platform
         from android.firstplay as a inner join a1 as b on safe_cast(a.video_id as int64)=b.video_id
         union all
         select a.timestamp,
                series,
                season,
                title,
                episode,
                b.video_id,
                user_id,
                'IOS' as platform
         from ios.firstplay as a inner join a1 as b on safe_cast(a.video_id as int64)=b.video_id
         union all
         select a.timestamp,
                series,
                season,
                b.title,
                episode,
                b.video_id,
                user_id,
                'Web' as platform
         from javascriptfirstplay as a inner join a1 as b on trim(upper(b.title))=trim(upper(a.title[safe_ordinal(1)])) )

select distinct
       d.timestamp,
       status,
       d.platform,
       customers.platform as source,
       user_id,
       case when series like '%Heartland%' then 1 else 0 end as Heartland,
       case when series like '%Bringing Up Bates%' then 1 else 0 end as Bringing_Up_Bates,
       case when (series not like '%Heartland%' and series not like '%Bringing Up Bates%') and season <> 'Movie' then 1 else 0 end as Other_Series,
       case when season='Movie' then 1 else 0 end as Movies
from d left join customers.customers on safe_cast(user_id as int64)=safe_cast(customer_id as int64)
where user_id<>'0' ;;
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

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

dimension: watched_heartland {
  type: number
  sql: ${TABLE}.heartland ;;
}

dimension: watched_bringing_up_bates {
  type: number
  sql: ${TABLE}.bringing_up_bates ;;
}

dimension: watched_other_series {
  type: number
  sql:  ${TABLE}.other_series;;
}

dimension: watched_movies {
  type: number
  sql: ${TABLE}.movies ;;
}

  measure: watched_heartland_total {
    type: sum
    sql: ${TABLE}.heartland ;;
  }

  measure: watched_bringing_up_bates_total {
    type: sum
    sql: ${TABLE}.bringing_up_bates ;;
  }

  measure: watched_other_series_total {
    type: sum
    sql:  ${TABLE}.other_series;;
  }

  measure: watched_movies_total {
    type: sum
    sql: ${TABLE}.movies ;;
  }

    }
