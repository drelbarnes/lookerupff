view: appletv {
  derived_table: {
    sql: with a as
(select content_type,
content_id,
pub_date,
title,
description,
genre,
rating,
advisory,
credit_name,
credit_role,
artwork_url,
artwork_type,
cast(isOriginal as int64) as isOriginal,
episodic_type,
tvshow_content_id,
tvseason_content_id,
season_number,
episode_number,
window_start,
window_end,
subscritpion_type
from svod_titles.AppleSeries
union all
select content_type,
content_id,
pub_date,
title,
description,
genre,
rating,
advisory,
credit_name,
credit_role,
artwork_url,
artwork_type,
cast(isOriginal as int64) as isOriginal,
episodic_type,
tvshow_content_id,
tvseason_content_id,
season_number,
episode_number,
window_start,
window_end,
subscritpion_type from svod_titles.AppleSeries_
union all
select content_type,
content_id,
pub_date,
title,
description,
genre,
rating,
advisory,
credit_name,
credit_role,
artwork_url,
artwork_type,
isOriginal,
episodic_type,
tvshow_content_id,
tvseason_content_id,
cast(season_number as int64) as season_number,
cast(episode_number as int64) as episode_number,
window_start,
window_end,
subscription_type from svod_titles.Launch_Planner_Movies_Apple
union all
select content_type,
content_id,
pub_date,
title,
description,
genre,
rating,
advisory,
credit_name,
credit_role,
artwork_url,
artwork_type,
isOriginal,
episodic_type,
tvshow_content_id,
tvseason_content_id,
cast(season_number as int64) as season_number,
cast(episode_number as int64) as episode_number,
window_start,
window_end,
subscription_type from svod_titles.Launch_Planner_Movies_Apple_),

b as
(select title,
       content_id
from a
where content_type='tv_show')

select a.*,
       case when b.content_id is not null then b.title else a.title end as series_movie_Title
from a left join b on substr(a.content_id,1,3) = b.content_id
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: series_movie_title {
    type: string
    sql: ${TABLE}.series_movie_title ;;
  }

  dimension: content_type {
    type: string
    sql: ${TABLE}.content_type ;;
  }

  dimension: content_id {
    type: string
    sql: ${TABLE}.content_id ;;
  }

  dimension: pub_date {
    type: string
    sql: ${TABLE}.pub_date ;;
  }

  dimension: title {
    type: string
    sql: case when ${content_type}='movie' then null else ${TABLE}.title end ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: genre {
    type: string
    sql: ${TABLE}.genre ;;
  }

  dimension: rating {
    type: string
    sql: ${TABLE}.rating ;;
  }

  dimension: advisory {
    type: string
    sql: ${TABLE}.advisory ;;
  }

  dimension: credit_name {
    type: string
    sql: ${TABLE}.credit_name ;;
  }

  dimension: credit_role {
    type: string
    sql: ${TABLE}.credit_role ;;
  }

  dimension: artwork_url {
    type: string
    sql: ${TABLE}.artwork_url ;;
  }

  dimension: artwork_type {
    type: string
    sql: ${TABLE}.artwork_type ;;
  }

  dimension: is_original {
    type: number
    sql: ${TABLE}.isOriginal ;;
  }

  dimension: episodic_type {
    type: string
    sql: ${TABLE}.episodic_type ;;
  }

  dimension: tvshow_content_id {
    type: string
    sql: ${TABLE}.tvshow_content_id ;;
  }

  dimension: tvseason_content_id {
    type: string
    sql: ${TABLE}.tvseason_content_id ;;
  }

  dimension: season_number {
    type: number
    sql: ${TABLE}.season_number ;;
  }

  dimension: episode_number {
    type: number
    sql: ${TABLE}.episode_number ;;
  }

  dimension: window_start {
    type: date
    sql: ${TABLE}.window_start ;;
  }

  dimension: window_end {
    type: date
    sql: ${TABLE}.window_end ;;
  }

  dimension: subscritpion_type {
    type: string
    sql: ${TABLE}.subscritpion_type ;;
  }

  set: detail {
    fields: [
      content_type,
      content_id,
      pub_date,
      title,
      description,
      genre,
      rating,
      advisory,
      credit_name,
      credit_role,
      artwork_url,
      artwork_type,
      is_original,
      episodic_type,
      tvshow_content_id,
      tvseason_content_id,
      season_number,
      episode_number,
      window_start,
      window_end,
      subscritpion_type
    ]
  }
}
