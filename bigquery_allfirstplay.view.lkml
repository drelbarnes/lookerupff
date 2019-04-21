view: bigquery_allfirstplay {
  derived_table: {
    sql:  with aa as
(select user_id,status_date as churn_date
from http_api.purchase_event
where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

bb as
(select user_id, max(status_date) as status_date
from http_api.purchase_event
where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
group by 1),

cc as
(select distinct bb.user_id
from aa inner join bb on aa.user_id=bb.user_id and status_date>churn_date),

a1 as
(select sent_at as timestamp,
        user_id,
        (split(title," - ")) as title
from javascript.firstplay),

a2 as
(select timestamp,
        user_id,
        title[safe_ordinal(1)] as title,
        concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
 from a1 order by 1),

titles_id_mapping as
(select *
from svod_titles.titles_id_mapping
where collection not in ('Romance - OLD',
'Dramas',
'Comedies',
'Kids - OLD',
'Christmas',
'Just Added',
'Music',
'Faith Movies',
'Docs & Specials',
'Trending',
'Adventure',
'All Movies',
'All Series',
'Bonus Content',
'Drama Movies',
'Drama Series',
'Faith Favorites',
'Family Addition',
'Family Comedies',
'Fan Favorite Series',
'Fantasy',
'Kids',
'New',
'New Series',
'Romance',
'Sports',
'The Must-Watch List',
'UPlifting Reality',
'UP Original Movies and Series',
'UP Original Series'
)),

a32 as
(select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                mysql_roku_firstplays_video_id,
                user_id,
                '' as anonymousId,
                'firstplay' as event_type,
                UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
                CAST('1111' AS int64) as platform_id
from looker.roku_firstplays),

a as
        (select sent_at as timestamp,
                b.date as release_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when series is null and upper(collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                event as event_type,
                'Android' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode
         from android.firstplay as a left join titles_id_mapping as b on a.video_id = b.id

         union all

        select timestamp,
       b.date,
       case when b.collection in ('Season 1','Season 2','Season 3') then concat(b.series,' ',b.collection) else collection end as collection,
       case when b.series is null and upper(b.collection)=upper(b.title) then 'movie'
                     when b.series is not null then 'series' else 'other' end as type,
       mysql_roku_firstplays_video_id as video_id,
       trim(b.title) as title,
       user_id,
      'anonymous_id' as anonymous_id,
       'firstplay' as event_type,
      'Roku' as source,
      UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
      CAST('1111' AS int64) as platform_id,
       b.episode
from a32 as a left join titles_id_mapping as b on mysql_roku_firstplays_video_id=b.id


         union all

         select sent_at as timestamp,
                b.date as release_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when series is null and upper(collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                event as event_type,
                'iOS' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode
         from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
         union all
         select sent_at as timestamp,
                b.date as release_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when series is null and upper(collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'Web' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode
         from javascript.loadedmetadata as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

        select timestamp,
                b.date as release_date,
                case when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection) else b.collection end as collection,
                case when series is null and upper(b.collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(b.id as int64) as video_id,
                trim(b.title) as title,
                user_id,
                '' as anonymous_id,
                'firstplay' as event_type,
                'Web' as source,
                UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
                CAST('33064' AS int64) as platform_id,
                episode
         from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))


select a.user_id,
       a.anonymous_id,
       a.event_type,
       a.timestamp,
       a.EPOCH_TIMESTAMP,
       a.platform_id,
       a.release_date,
       a.collection,
       a.type,
       a.video_id,
       a.title,
       a.source,
       a.episode,
       case when cc.user_id is null then 'first-time customers' else 'reacquisitions' end as winback,
       case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
            else "NA"
            end as Quarter
from a left join cc on a.user_id=cc.user_id
where a.user_id<>'0'    ;;
  }

dimension: winback {
  type: string
  sql: ${TABLE}.winback ;;
}

  dimension: quarter {
    type: string
    sql: ${TABLE}.quarter ;;
  }

  dimension: current_date {
    type: date
    sql: current_date() ;;
  }

  dimension_group: current_date_ {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      week,
      month,
      quarter,
      year
    ]
    sql: current_date() ;;
    }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  measure: title_count {
    type: count_distinct
    sql: ${video_id} ;;
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

  measure: episode_count {
    type: max
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

  dimension: platform_id {
    type: number
    sql: ${TABLE}.platform_id ;;
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

  dimension: anonymousId {
    type: string
    tags: ["segment_anonymous_id"]
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: epoch_timestamp {
    type: number
    value_format: "0"
    sql: ${TABLE}.epoch_timestamp ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: itemId {
    type: number
    value_format: "0"
    sql: ${video_id} ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
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

  measure: play_count {
    type: count_distinct
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${source};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: views_per_user {
    type: number
    sql: 1.0*${play_count}/${user_count} ;;
    value_format: "0.0"
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      title,
      platform,
      user_id
    ]
  }

# ------
# Filters
# ------

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: no
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }



## filter determining time range for all "B" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: no
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: time_c {
    type: date_time
  }
## flag for "C" measures to only include appropriate time range
  dimension: group_c {
    hidden: no
    type: yesno
    sql: {% condition time_c %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: time_d {
    type: date_time
  }

## flag for "D" measures to only include appropriate time range
  dimension: group_d {
    hidden: no
    type: yesno
    sql: {% condition time_d %} ${timestamp_raw} {% endcondition %}
      ;;
  }



  measure: plays_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: plays_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: plays_c {
    type: count_distinct
    filters: {
      field: group_c
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: plays_d {
    type: count_distinct
    filters: {
      field: group_d
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }


  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  filter:  promotional_id{
    type: number
  }

  dimension: promotional {
    hidden: no
    type: yesno
    sql: {%condition promotional_id%} ${video_id} {%endcondition%};;
  }

  measure: promotional_plays {
    type: count_distinct
    filters: {
      field: promotional
      value: "yes"
    }
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string))  ;;
  }

  measure: promotional_title_count {
    type: count_distinct
    filters: {
      field: promotional
      value: "yes"
    }
    sql: ${video_id} ;;
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }

}
